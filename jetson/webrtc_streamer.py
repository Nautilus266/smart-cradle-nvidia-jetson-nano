#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import threading
import traceback
import signal
import threading
import gc
import re
from dotenv import load_dotenv

import firebase_admin
from firebase_admin import credentials, db

import gi
gi.require_version("Gst", "1.0")
gi.require_version("GstSdp", "1.0")
gi.require_version("GstWebRTC", "1.0")
gi.require_version("GstVideo", "1.0")
from gi.repository import Gst, GstSdp, GstWebRTC, GLib, GstVideo

# ===================== SEGURIDAD Y CONFIG =====================
# Cargar variables de entorno ocultas
load_dotenv()


# Validar dependencias críticas
SERVICE_ACCOUNT = os.getenv("FIREBASE_SERVICE_ACCOUNT")
FIREBASE_DB_URL = os.getenv("FIREBASE_DB_URL")

if not SERVICE_ACCOUNT or not FIREBASE_DB_URL:
    raise SystemExit("ERROR: Faltan credenciales de Firebase en el archivo .env")

ROOM_ID         = os.getenv("ROOM_ID", "default_room")
STUN_SERVER     = os.getenv("STUN_SERVER", "stun://stun.l.google.com:19302")

# Servidores TURN (Pueden venir vacíos si se usa solo STUN en red local)
TURN_SERVER     = os.getenv("TURN_SERVER_UDP", "")
TURN_SERVER_TCP = os.getenv("TURN_SERVER_TCP", "")

# Hardware de Video
VIDEO_DEV       = os.getenv("VIDEO_DEV", "/dev/video0")
VIDEO_FRAMERATE = "15/1"
VIDEO_WIDTH     = 640
VIDEO_HEIGHT    = 360
VIDEO_BITRATE   = 400_000  # bps

# MJPEG térmica local (server Flask que corre en la Jetson)
THERMAL_URL = os.getenv("THERMAL_URL", "http://127.0.0.1:5000/thermal.mjpg")
THERMAL_FPS = "10/1"

# ---- Parches anti “m-lines order” ----
ENABLE_NONCE_REOFFER = False
NONCE_DEBOUNCE_MS = 1500
# =====================================

Gst.init(None)

# ---------- Firebase init ----------
cred = credentials.Certificate(SERVICE_ACCOUNT)
firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})

root_ref = db.reference("/")
call_ref = root_ref.child("calls").child(ROOM_ID)

offer_ref       = call_ref.child("offer")
answer_ref      = call_ref.child("answer")
caller_cand_ref = call_ref.child("callerCandidates")
callee_cand_ref = call_ref.child("calleeCandidates")

callee_stream = None

controls_ref         = call_ref.child("controls")
controls_active_ref  = controls_ref.child("active")   # bool
controls_thermal_ref = controls_ref.child("thermal")  # bool

# clientNonce dedicado para evitar que el callee reciba ICE de un offer anterior
nonce_ref = call_ref.child("clientNonce")

# ---------- GStreamer globals ----------
pipeline        = None
webrtc          = None
video_encoder   = None
video_parser    = None
video_payloader = None
streaming_active= False
video_appsrc= None

sel_video   = None
valve_cam   = None
valve_therm = None

LOCAL_MIDS = []

# audio RX (teléfono->Jetson)
rtpjbuf_rx  = None
rtpopusdep  = None
opusdec     = None
aconv_rx    = None
ares_rx     = None
asink_rx    = None

# ---------- State ----------
controls_active  = False
controls_thermal = False

_last_offer_ms = 0
OFFER_DEBOUNCE_MS = 1500

# ------- Anti multi-offer -------
CURRENT_OFFER_ID = None
LAST_ANSWER_SDP = None
OFFER_IN_FLIGHT = False   # ya hay una offer en proceso
ANSWER_SET      = False   # ya se aplicó answer -> no se vuelve a ofertar
LOCAL_OFFER_PUBLISHED = False  # no procesar ICE del callee hasta publicar offer
LAST_NONCE      = None
_last_nonce_ms  = 0
# ----------------------------

streams = []  # firebase streams para cerrar en exit


def now_ms():
    return int(time.time() * 1000)


def parse_bool(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes", "y", "on"):
            return True
        if s in ("false", "0", "no", "n", "off"):
            return False
    # fallback
    return bool(v)


def gst_call(fn, *args, **kwargs):
    """Ejecuta fn(...) en el hilo del mainloop de GLib (thread-safe)."""
    def _wrap():
        try:
            fn(*args, **kwargs)
        except Exception:
            traceback.print_exc()
        return False
    GLib.idle_add(_wrap, priority=GLib.PRIORITY_HIGH)


def must_make(factory, name=None):
    e = Gst.ElementFactory.make(factory, name)
    if e is None:
        raise RuntimeError(f"No se pudo crear elemento GStreamer: {factory}")
    return e


def safe_set(element, prop, value):
    try:
        element.set_property(prop, value)
        print(f"[GST] {element.get_name()}.{prop} = {value}")
    except Exception as e:
        print(f"[GST][WARN] {element.get_name()} sin prop '{prop}': {e}")


def link_many(*elements):
    for a, b in zip(elements, elements[1:]):
        if not a.link(b):
            raise RuntimeError(f"Falló link {a.get_name()} -> {b.get_name()}")
    return True


# ===================== DEBUG PROBES =====================
DEBUG_PROBES = True
PROBE_EVERY_N = 30  # imprime cada N buffers

def _fmt_time_ns(ns):
    if ns is None or ns == Gst.CLOCK_TIME_NONE:
        return "NONE"
    return f"{ns/1e9:.3f}s"

def add_caps_event_probe(pad: Gst.Pad, label: str):
    if not DEBUG_PROBES or pad is None:
        return
    def _probe(pad, info):
        ev = info.get_event()
        if not ev:
            return Gst.PadProbeReturn.OK
        t = ev.type
        if t == Gst.EventType.CAPS:
            caps = ev.parse_caps()
            print(f"[PROBE][{label}] CAPS -> {caps.to_string()}")
        elif t == Gst.EventType.STREAM_START:
            sid = ev.parse_stream_start()
            print(f"[PROBE][{label}] STREAM_START -> {sid}")
        elif t == Gst.EventType.SEGMENT:
            print(f"[PROBE][{label}] SEGMENT")
        return Gst.PadProbeReturn.OK
    pad.add_probe(Gst.PadProbeType.EVENT_DOWNSTREAM, _probe)

def add_buffer_counter_probe(pad: Gst.Pad, label: str, every=PROBE_EVERY_N):
    if not DEBUG_PROBES or pad is None:
        return
    counter = {"n": 0, "t0": time.time(), "bytes": 0}
    def _probe(pad, info):
        buf = info.get_buffer()
        if not buf:
            return Gst.PadProbeReturn.OK
        counter["n"] += 1
        counter["bytes"] += buf.get_size()
        if counter["n"] % every == 0:
            dt = max(time.time() - counter["t0"], 1e-6)
            fps = counter["n"] / dt
            kbps = (counter["bytes"] * 8) / dt / 1000.0
            print(f"[PROBE][{label}] buffers={counter['n']} fps~{fps:.1f} kbps~{kbps:.1f} "
                  f"pts={_fmt_time_ns(buf.pts)} dts={_fmt_time_ns(buf.dts)} size={buf.get_size()}")
        return Gst.PadProbeReturn.OK
    pad.add_probe(Gst.PadProbeType.BUFFER, _probe)

def add_pad_debug(element: Gst.Element, pad_name: str, label: str):
    if not DEBUG_PROBES or element is None:
        return
    pad = element.get_static_pad(pad_name)
    if not pad:
        print(f"[PROBE][WARN] {label}: no existe pad {element.get_name()}:{pad_name}")
        return
    add_caps_event_probe(pad, label)
    add_buffer_counter_probe(pad, label)
# =======================================================

def choose_h264_encoder():
    for enc_name in ("nvv4l2h264enc", "omxh264enc", "x264enc"):
        e = Gst.ElementFactory.make(enc_name)
        if e:
            print(f"[GST] Usando encoder: {enc_name}")
            return e, enc_name
    raise RuntimeError("No hay encoder H.264 disponible")


# ============== Keyframe helpers ==============
def _send_fku(pad, tag=""):
    try:
        s = Gst.Structure.new_empty("GstForceKeyUnit")
        s.set_value("all-headers", True)
        s.set_value("count", 0)
        ev = Gst.Event.new_custom(Gst.EventType.CUSTOM_UPSTREAM, s)
        ok = pad.send_event(ev)
        print(f"[KF] FKU {tag} -> {ok}")
    except Exception as e:
        print(f"[KF] error {tag}: {e}")


def force_keyframe(tag=""):
    global video_encoder
    try:
        if video_encoder:
            # Se envía evento DOWNSTREAM al sink del encoder
            pad = video_encoder.get_static_pad("src")
            if pad:
                # Se crea un evento oficial de GStreamer para forzar Keyframe }
                ev = GstVideo.video_event_new_upstream_force_key_unit( 
                    Gst.CLOCK_TIME_NONE, True, 0 
                ) 
                pad.send_event(ev) 
                print(f"[KF] ¡BOOM! Keyframe forzado exitosamente ({tag})")
    except Exception as e:
        print(f"[KF] Error: {e}")




def schedule_keyframes(tag=""):
    gst_call(force_keyframe, tag + "#0")
    GLib.timeout_add(1000, lambda: (force_keyframe(tag + "#1") or False))
    GLib.timeout_add(2500, lambda: (force_keyframe(tag + "#2") or False))

def is_usable_ice_candidate(cand: str) -> bool:
    c = cand.lower()

    # permite TURN y srflx SIEMPRE
    if " typ relay" in c or " typ srflx" in c:
        return True

    # IPv6 link-local
    if " fe80:" in c:
        return False

    # docker host candidate inútil
    if " 172.17." in c:
        return False

    # LAN real
    if " 192.168." in c:
        return True

    # si no se sabe, se deja pasar
    return True



# ===================== AUDIO TX (Jetson -> teléfono) =====================
def build_audio_send():
    asrc = None
    for cand in ("alsasrc", "pulsesrc", "autoaudiosrc"):
        e = Gst.ElementFactory.make(cand)
        if e:
            asrc = e
            break
    if asrc is None:
        raise RuntimeError("No hay fuente de audio (alsasrc/pulsesrc/autoaudiosrc)")

    aconv = must_make("audioconvert")
    ares  = must_make("audioresample")
    opus  = must_make("opusenc")
    asrc = must_make("alsasrc", "alsasrc0")
    safe_set(asrc, "device", "hw:2")
    safe_set(asrc, "do-timestamp", True) 
    safe_set(asrc, "is-live", True)

    # --- Filtros de Ruido y Eco de WebRTC --- 
    wdsp = must_make("webrtcdsp", "wdsp") 
    safe_set(wdsp, "noise-suppression-level", 3) # 3 = Nivel máximo de supresión de ruido (High) 
    safe_set(wdsp, "echo-cancellation", True) 
    safe_set(wdsp, "high-pass-filter", True) # Quita zumbidos eléctricos de baja frecuencia 

    # Después del DSP, a veces se necesita reconvertir el formato para el opusenc 
    aconv2 = must_make("audioconvert", "aconv2") 
    ares2 = must_make("audioresample", "ares2")

    q_audio = must_make("queue", "q_audio") 
    safe_set(q_audio, "leaky", 2)
    safe_set(q_audio, "max-size-buffers", 10)

    safe_set(opus, "bitrate", 32000)
    safe_set(opus, "frame-size", 20)
    safe_set(opus, "complexity", 3)
    pay   = must_make("rtpopuspay")
    safe_set(pay, "pt", 111)

    capsf = must_make("capsfilter")
    capsf.set_property("caps", Gst.Caps.from_string(
        "application/x-rtp,media=audio,encoding-name=OPUS,payload=111,clock-rate=48000"
    ))

    for e in (asrc, q_audio, aconv, ares, opus, pay, capsf):
        pipeline.add(e)
    link_many(asrc, q_audio, aconv, ares, opus, pay, capsf)

    webrtc_sinkpad = webrtc.get_request_pad("sink_%u")
    print(f"[LINK] AUDIO RTP -> webrtcbin:{webrtc_sinkpad.get_name()}")
    capsf.get_static_pad("src").link(webrtc_sinkpad)

    # probes útiles
    add_pad_debug(capsf, "src", "AUDIO capsf:src")



# ===================== AUDIO RX (teléfono -> Jetson) =====================
def attach_audio_receive(pad):
    global rtpjbuf_rx, rtpopusdep, opusdec, aconv_rx, ares_rx, asink_rx

    if rtpjbuf_rx is None:
        print("[AUDIO][RX] Construyendo rama de audio dinámicamente...")
        rtpjbuf_rx = must_make("rtpjitterbuffer", "rtpjbuf_rx")
        rtpopusdep = must_make("rtpopusdepay", "rtpopusdep")
        opusdec    = must_make("opusdec", "opusdec_rx")
        aconv_rx   = must_make("audioconvert", "aconv_rx")
        ares_rx    = must_make("audioresample", "ares_rx")

        asink_rx = must_make("alsasink", "asink_rx")
        safe_set(asink_rx, "device", "plughw:2")
        safe_set(asink_rx, "sync", False) 
        
        # Evita que GStreamer se congele al conectar inmediatamente
        safe_set(asink_rx, "async", False) 

        for e in (rtpjbuf_rx, rtpopusdep, opusdec, aconv_rx, ares_rx, asink_rx):
            pipeline.add(e)
            e.sync_state_with_parent()

        link_many(rtpjbuf_rx, rtpopusdep, opusdec, aconv_rx, ares_rx, asink_rx)

    sinkpad = rtpjbuf_rx.get_static_pad("sink")
    if not sinkpad.is_linked():
        res = pad.link(sinkpad)
        print(f"[AUDIO][RX] Enlace de audio completado (Resultado: {res})")



# ===================== VIDEO (Encoder Software) =====================

def build_video_send():
    global video_parser, video_payloader, video_encoder
    global sel_video, valve_cam, valve_therm
    
    print("[VIDEO] Configurando Pipeline Dual (Cámara C920 + Térmica)...")


    # RAMA CÁMARA NORMAL (C920)
   
    vsrc = must_make("v4l2src", "v4l2src0")
    safe_set(vsrc, "device", VIDEO_DEV)
    safe_set(vsrc, "do-timestamp", True) 
    safe_set(vsrc, "is-live", True)

    caps_cam = must_make("capsfilter", "caps_cam")
    caps_cam.set_property("caps", Gst.Caps.from_string("image/jpeg, width=640, height=360, framerate=15/1"))

    dec_cam = must_make("jpegdec", "jpegdec_cam") 
    conv_cam = must_make("videoconvert", "vconv_cam") 

    valve_cam = must_make("valve", "valve_cam")
    safe_set(valve_cam, "drop", False) # Pasa por defecto (se deja fluir el video)

    q_cam = must_make("queue", "q_cam")
    safe_set(q_cam, "leaky", 2)
    safe_set(q_cam, "max-size-buffers", 1)
    
    # RAMA TÉRMICA (AMG8833 vía Flask)

    tsrc = must_make("souphttpsrc", "therm_src")
    safe_set(tsrc, "location", THERMAL_URL)
    safe_set(tsrc, "is-live", True)
    
    tdemux = must_make("multipartdemux", "therm_demux")
    tdec = must_make("jpegdec", "therm_dec")
    tconv = must_make("videoconvert", "therm_conv")
    
    valve_therm = must_make("valve", "valve_therm")
    safe_set(valve_therm, "drop", True) # Bloqueada por defecto ahorrando CPU (se activa solo si el usuario lo pide)
    
    q_therm = must_make("queue", "q_therm")
    safe_set(q_therm, "leaky", 2)
    safe_set(q_therm, "max-size-buffers", 1)

    # SELECTOR DE VIDEO (El "Switch")

    sel_video = must_make("input-selector", "sel_video")
    safe_set(sel_video, "sync-streams", True) # Evita saltos de tiempo al cambiar
    safe_set(sel_video, "sync-mode", 1)       # Sincronización al reloj (Clock)

    # ENCODER Y WEBRTC (El Tronco Común)
    
    nvconv = must_make("nvvidconv", "nv_conv0") 
    caps_nvmm = must_make("capsfilter", "caps_nvmm")
    # Forzamos que, sin importar qué cámara entre, salga a 640x360 para no confundir al encoder de la Jetson
    caps_nvmm.set_property("caps", Gst.Caps.from_string("video/x-raw(memory:NVMM), format=I420, width=640, height=360"))

    q_preenc = must_make("queue", "q_video_preenc")
    safe_set(q_preenc, "leaky", 2)
    safe_set(q_preenc, "max-size-buffers", 1)
    safe_set(q_preenc, "max-size-bytes", 0)
    safe_set(q_preenc, "max-size-time", 0)

    nvenc = must_make("nvv4l2h264enc", "nv_h264_enc")
    safe_set(nvenc, "bitrate", 800000)
    safe_set(nvenc, "insert-sps-pps", True)
    safe_set(nvenc, "insert-aud", True)
    safe_set(nvenc, "idrinterval", 15)

    caps_h264 = must_make("capsfilter", "caps_h264")
    caps_h264.set_property("caps", Gst.Caps.from_string("video/x-h264, profile=(string)baseline, level=(string)3.1"))

    hparse = must_make("h264parse", "h264parse0")
    safe_set(hparse, "config-interval", -1)

    pay = must_make("rtph264pay", "rtph264pay0")
    safe_set(pay, "pt", 96)
    safe_set(pay, "mtu", 1200)
    safe_set(pay, "config-interval", 1)

    caps_rtp = must_make("capsfilter", "caps_rtp")
    caps_rtp.set_property("caps", Gst.Caps.from_string(
        "application/x-rtp, media=(string)video, encoding-name=(string)H264, "
        "payload=(int)96, clock-rate=(int)90000, packetization-mode=(string)1"
    ))

    q_postrtp = must_make("queue", "q_video_postrtp")
    safe_set(q_postrtp, "leaky", 0)
    safe_set(q_postrtp, "max-size-buffers", 0)
    safe_set(q_postrtp, "max-size-bytes", 0)
    safe_set(q_postrtp, "max-size-time", 0)

    # --- AGREGAR TODOS LOS ELEMENTOS AL PIPELINE ---
    elements_to_add = [
        vsrc, caps_cam, dec_cam, conv_cam, valve_cam, q_cam,
        tsrc, tdemux, tdec, tconv, valve_therm, q_therm,
        sel_video, nvconv, caps_nvmm, q_preenc, nvenc, caps_h264, hparse, pay, caps_rtp, q_postrtp
    ]
    for e in elements_to_add:
        if e.get_parent() is None:
            pipeline.add(e)

    def link_or_die(a, b):
        ok = a.link(b)
        if not ok:
            raise RuntimeError(f"[LINK][FAIL] {a.get_name()} -> {b.get_name()}")
        print(f"[LINK][OK] {a.get_name()} -> {b.get_name()}")

    # --- ENLAZAR RAMA 1 (Cámara Normal) ---
    link_or_die(vsrc, caps_cam)
    link_or_die(caps_cam, dec_cam)
    link_or_die(dec_cam, conv_cam)
    link_or_die(conv_cam, valve_cam)
    link_or_die(valve_cam, q_cam)
    
    # El selector pide un "sink" dinámico. El primero será sink_0
    cam_pad = sel_video.get_request_pad("sink_%u") 
    res_cam = q_cam.get_static_pad("src").link(cam_pad)
    if res_cam != Gst.PadLinkReturn.OK:
        raise RuntimeError(f"[LINK][FAIL] q_cam -> sel_video (sink_0)")

    # --- ENLAZAR RAMA 2 (Térmica) ---
    link_or_die(tsrc, tdemux) # El HTTP Source va directo al Demuxer
    
    # El multipartdemux crea sus salidas dinámicamente según recibe internet, por lo que hay que enlazar el pad dinámico con el resto de la rama
    def on_demux_pad_added(demux, pad):
        print("[THERM] Enlazando pad dinámico del stream térmico...")
        sink_pad = tdec.get_static_pad("sink")
        if not sink_pad.is_linked():
            pad.link(sink_pad)
        
    tdemux.connect("pad-added", on_demux_pad_added)
    
    link_or_die(tdec, tconv)
    link_or_die(tconv, valve_therm)
    link_or_die(valve_therm, q_therm)
    
    # El segundo será sink_1
    therm_pad = sel_video.get_request_pad("sink_%u") 
    res_therm = q_therm.get_static_pad("src").link(therm_pad)
    if res_therm != Gst.PadLinkReturn.OK:
        raise RuntimeError(f"[LINK][FAIL] q_therm -> sel_video (sink_1)")

    # --- ENLAZAR TRONCO COMÚN ---
    sel_video.set_property("active-pad", cam_pad) # Se ve la web normal
    
    link_or_die(sel_video, nvconv)
    link_or_die(nvconv, caps_nvmm)
    link_or_die(caps_nvmm, q_preenc)
    link_or_die(q_preenc, nvenc)
    link_or_die(nvenc, caps_h264)
    link_or_die(caps_h264, hparse)
    link_or_die(hparse, pay)
    link_or_die(pay, caps_rtp)
    link_or_die(caps_rtp, q_postrtp)

    # --- CONECTAR AL WEBRTC ---
    webrtc_sink_1 = webrtc.get_request_pad("sink_1")
    if not webrtc_sink_1:
        raise RuntimeError("[ERR] No se pudo pedir webrtcbin:sink_1")

    res = q_postrtp.get_static_pad("src").link(webrtc_sink_1)
    if res != Gst.PadLinkReturn.OK:
        raise RuntimeError(f"[LINK][FAIL] q_postrtp:src -> webrtcbin:sink_1 = {res}")

    video_encoder = nvenc
    video_parser = hparse
    video_payloader = pay
    print("[VIDEO] Pipeline Dual construida con éxito.")


def apply_thermal_state():
    global sel_video, valve_cam, valve_therm, controls_thermal
    
    if not sel_video or not valve_cam or not valve_therm:
        return

    # Se dejan ambas llaves abiertas siempre. 
    # El flujo de tiempo nunca se detiene, evitando que Android colapse.
    safe_set(valve_therm, "drop", False)
    safe_set(valve_cam, "drop", False)

    if controls_thermal:
        ap = sel_video.get_static_pad("sink_1")
        if ap:
            sel_video.set_property("active-pad", ap)
        print("[THERM] ON (Sensor térmico activado - Transición limpia)")
    else:
        ap = sel_video.get_static_pad("sink_0")
        if ap:
            sel_video.set_property("active-pad", ap)
        print("[THERM] OFF (Cámara web activada - Transición limpia)")

    try:
        schedule_keyframes("therm-toggle")
    except Exception as e:
        print("[THERM] Aviso: No se pudo forzar keyframe:", e)

# ===================== SDP helpers =====================
def sdp_from_text(t: str):
    ok, msg = GstSdp.SDPMessage.new()
    if ok != GstSdp.SDPResult.OK:
        raise RuntimeError("SDPMessage.new() failed")
    ok = GstSdp.sdp_message_parse_buffer(t.encode("utf-8"), msg)
    if ok != GstSdp.SDPResult.OK:
        raise RuntimeError("sdp_message_parse_buffer failed")
    return msg

def normalize_sdp_for_android(sdp: str) -> str:
    sdp = sdp.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.strip() for ln in sdp.split("\n") if ln.strip()]

    new_lines = []
    in_audio = False
    for ln in lines:
        if ln.startswith("m=audio"):
            in_audio = True
        elif ln.startswith("m=video"):
            in_audio = False
        
        # Se fuerza el audio a sendrecv para engañar a Android
        if in_audio and (ln == "a=sendonly" or ln == "a=recvonly"):
            new_lines.append("a=sendrecv")
        else:
            new_lines.append(ln)

    return "\r\n".join(new_lines) + "\r\n"

# ===================== Offer creation / publish =====================
def publish_offer(sdp_text: str):
    global LOCAL_OFFER_PUBLISHED, CURRENT_OFFER_ID, LOCAL_MIDS

    LOCAL_MIDS = []
    for ln in sdp_text.splitlines():
        if ln.startswith("a=mid:"):
            LOCAL_MIDS.append(ln.split(":", 1)[1].strip())
    print("[SDP] mids =", LOCAL_MIDS)

    if CURRENT_OFFER_ID is None:
        CURRENT_OFFER_ID = now_ms()

    offer_ref.set({
        "type": "offer",
        "sdp": sdp_text,
        "offerId": CURRENT_OFFER_ID
    })

    LOCAL_OFFER_PUBLISHED = True
    print(f"[Firebase] OFFER publicada (offerId={CURRENT_OFFER_ID})")

def on_offer_created(promise, element):
    global _last_offer_ms, OFFER_IN_FLIGHT
    try:
        reply = promise.get_reply()
        offer = reply.get_value("offer")
        
        raw_sdp = offer.sdp.as_text()
        fixed_sdp_text = normalize_sdp_for_android(raw_sdp)
        
        fixed_sdp_msg = sdp_from_text(fixed_sdp_text)
        fixed_offer = GstWebRTC.WebRTCSessionDescription.new(GstWebRTC.WebRTCSDPType.OFFER, fixed_sdp_msg)
        
        element.emit("set-local-description", fixed_offer, Gst.Promise.new())
        publish_offer(fixed_sdp_text)
        
        _last_offer_ms = now_ms()
        schedule_keyframes("after-offer")
    except Exception:
        print("[OFFER][ERROR]")
        traceback.print_exc()
    finally:
        OFFER_IN_FLIGHT = False


def request_offer(tag=""):
    global webrtc, _last_offer_ms, OFFER_IN_FLIGHT, ANSWER_SET, LOCAL_OFFER_PUBLISHED, CURRENT_OFFER_ID
    
    if CURRENT_OFFER_ID is None:
        CURRENT_OFFER_ID = now_ms()

    if webrtc is None:
        print("[OFFER] webrtc=None, ignoro")
        return

    if not controls_active:
        print("[OFFER] controls_active=False, ignoro")
        return

    if ANSWER_SET:
        print(f"[OFFER] IGNORE ({tag}) ya hay ANSWER_SET=True")
        return

    if OFFER_IN_FLIGHT:
        print(f"[OFFER] IGNORE ({tag}) OFFER_IN_FLIGHT=True")
        return

    if now_ms() - _last_offer_ms < OFFER_DEBOUNCE_MS:
        print(f"[OFFER] debounce ({tag}), ignoro")
        return

    OFFER_IN_FLIGHT = True
    LOCAL_OFFER_PUBLISHED = False  # rearm gating

    p = Gst.Promise.new_with_change_func(on_offer_created, webrtc)
    webrtc.emit("create-offer", None, p)
    print(f"[OFFER] solicitada ({tag})")


# ===================== Pipeline lifecycle =====================

def on_ice_connection_state_change(element, pspec):
    # Se obtiene el estado actual de la conexión
    state = element.get_property('ice-connection-state')
    print(f"[WEBRTC] Cambio de estado ICE: {state}")

    # 3 = CONNECTED, 4 = COMPLETED
    if state == 3 or state == 4 or "CONNECTED" in str(state) or "COMPLETED" in str(state):
        print("[WEBRTC] ¡Conexión P2P establecida! Aniquilando fantasmas en Firebase...")
        
        # Se borran los datos de señalización.
        # Se hacen en un hilo en segundo plano para no interrumpir el flujo del video.
        # Al dejar Firebase vacío desde este momento, la próxima vez que Android entre a la pantalla, se verá obligado a esperar la oferta nueva.
        threading.Thread(target=cleanup_firebase_session, daemon=True).start()



def build_pipeline():
    global pipeline, webrtc, video_encoder

    pipeline = Gst.Pipeline.new("webrtc-pipe")
    webrtc = must_make("webrtcbin", "webrtcbin")

    safe_set(webrtc, "ice-transport-policy", "all") # "realy" obliga a usar TURN

    # --- Se fuerza GStreamer a usar un solo transporte (BUNDLE) ---

    safe_set(webrtc, "bundle-policy", 3) # 3 equivale a GstWebRTC.WebRTCBundlePolicy.MAX_BUNDLE


    if STUN_SERVER:
        webrtc.set_property("stun-server", STUN_SERVER)

    if TURN_SERVER:
        safe_set(webrtc, "turn-server", TURN_SERVER)

    webrtc.connect('notify::ice-connection-state', on_ice_connection_state_change)

    pipeline.add(webrtc)

    build_audio_send()

    build_video_send()

    # Señales webrtcbin
    def on_ice_candidate(element, mlineindex, candidate):
        try:
            if not is_usable_ice_candidate(candidate):
                print("[ICE][DROP] candidato inválido:", candidate)
                return

            # Mapeo fijo basado en tu offer: {0=audio0, 1=video1}
            # mid REAL desde SDP si existe
            sdp_mid = None
            try:
                if isinstance(mlineindex, int) and mlineindex < len(LOCAL_MIDS):
                    sdp_mid = LOCAL_MIDS[mlineindex]
            except Exception:
                pass
            if not sdp_mid:
                sdp_mid = str(mlineindex)

            # Tipo de candidate
            lc = candidate.lower()
            if " typ relay" in lc:
                ctyp = "relay"
            elif " typ srflx" in lc:
                ctyp = "srflx"
            else:
                ctyp = "host"

            print(f"[ICE][LOCAL] mline={mlineindex} mid={sdp_mid} type={ctyp} cand={candidate[:110]}...")

            data = {
                "candidate": candidate,
                "sdpMLineIndex": int(mlineindex),
                "sdpMid": sdp_mid,
                "offerId": CURRENT_OFFER_ID
            }


            caller_cand_ref.child(str(CURRENT_OFFER_ID)).push(data)

            print(f"[ICE][OK] caller -> offerId={CURRENT_OFFER_ID} mid={sdp_mid} mline={mlineindex}")
        except Exception as e:
            print("[Firebase][ICE][WARN]", e)


    webrtc.connect("on-ice-candidate", on_ice_candidate)

    def _notify(obj, pspec):
        name = pspec.name
        try:
            val = obj.get_property(name)
        except Exception:
            val = "<err>"
        print(f"[WEBRTC][STATE] {name} = {val}")

    for prop in ("ice-connection-state", "connection-state", "signaling-state", "ice-gathering-state"):
        try:
            webrtc.connect(f"notify::{prop}", _notify)
        except Exception as e:
            print(f"[WEBRTC][WARN] no pude enganchar notify::{prop}: {e}")


    def on_pad_added(element, pad):
        try:
            name = pad.get_name()
            print(f"\n[WEBRTC][PAD-ADDED] ¡Pad detectado!: {name}")
            
            # En lugar de buscar el nombre, se busca que sea un pad de salida (SRC)
            if pad.get_direction() != Gst.PadDirection.SRC:
                print(f"[AUDIO][RX] Ignorando pad auxiliar: {name}")
                return
                
            print("[AUDIO][RX] ¡Es el RTP de voz! Construyendo tubería a bocinas...")
            
            # Se creanelementos con la API oficial pura
            rtpjbuf = Gst.ElementFactory.make("rtpjitterbuffer", "rtpjbuf_rx")
            rtpopusdep = Gst.ElementFactory.make("rtpopusdepay", "rtpopusdep")
            opusdec = Gst.ElementFactory.make("opusdec", "opusdec_rx")
            aconv = Gst.ElementFactory.make("audioconvert", "aconv_rx")
            ares = Gst.ElementFactory.make("audioresample", "ares_rx")
            asink = Gst.ElementFactory.make("autoaudiosink", "asink_rx")
            
            if not all([rtpjbuf, rtpopusdep, opusdec, aconv, ares, asink]):
                print("[AUDIO][RX] ERROR: Faltan plugins de audio instalados.")
                return

            # Se configura la salida (Seguro anti-congelamiento ALSA)
            asink.set_property("sync", False)

            # Se añadea la pipeline viva
            global pipeline
            for e in [rtpjbuf, rtpopusdep, opusdec, aconv, ares, asink]:
                pipeline.add(e)
                e.sync_state_with_parent()

            # Se enlazanentre sí en cadena
            rtpjbuf.link(rtpopusdep)
            rtpopusdep.link(opusdec)
            opusdec.link(aconv)
            aconv.link(ares)
            ares.link(asink)

            # Se conecta el pad que acaba de salir de WebRTC directo a la cadena
            res = pad.link(rtpjbuf.get_static_pad("sink"))
            print(f"[AUDIO][RX] ¡Conexión terminada! Resultado: {res}")

        except Exception as e:
            print(f"\n[ERROR FATAL EN PAD-ADDED]: {e}")
            import traceback
            traceback.print_exc()

    # Indica a GStreamer que use la función de arriba
    webrtc.connect('pad-added', on_pad_added)


    def on_stats(promise, _ud):
        try:
            reply = promise.get_reply()
            if reply is None:
                print("[WEBRTC][STATS] reply=None")
                return

            # Imprime siempre la estructura completa
            try:
                print("[WEBRTC][STATS][REPLY]", reply.to_string())
            except Exception:
                print("[WEBRTC][STATS][REPLY]", reply)

            # Intentar leer "stats" si existe
            stats = None
            try:
                stats = reply.get_value("stats")
            except Exception:
                stats = None

            if stats is None:
                # Muestra qué campos trae realmente
                try:
                    fields = [reply.nth_field_name(i) for i in range(reply.n_fields())]
                    print("[WEBRTC][STATS] stats=None fields=", fields)
                except Exception:
                    pass
                return

            # stats puede no tener to_string()
            try:
                print("[WEBRTC][STATS][STATS]", stats.to_string())
            except Exception:
                print("[WEBRTC][STATS][STATS]", stats)

        except Exception:
            traceback.print_exc()


    def tick_stats():
        if webrtc is None:
            return False
        p = Gst.Promise.new_with_change_func(on_stats, None)
        try:
            webrtc.emit("get-stats", None, p)
        except Exception:
            traceback.print_exc()
        return True

    GLib.timeout_add_seconds(2, tick_stats)


    # Bus: si falla térmica, volvemos a cámara (no se detiene el streaming)
    bus = pipeline.get_bus()
    bus.add_signal_watch()

    
    def on_bus_message(bus, msg):
        mtype = msg.type
        src = msg.src.get_name() if msg.src else "unknown"

        if mtype == Gst.MessageType.ERROR:
            err, dbg = msg.parse_error()
            print("[GST][ERROR] from", src, ":", err, "| dbg:", dbg)

            # Si falla la rama térmica, se apaga y se vuelve a la cámara normal. No se detiene el streaming.
            if (src.startswith("souphttpsrc") or src.startswith("multipartdemux") or
                src.startswith("jpegdec1") or src.startswith("therm_") or
                src.startswith("nvvidconv2") or src == "caps_nvmm2"):
                print("[THERM][WARN] falló térmica -> vuelvo a cámara")
                try:
                    global controls_thermal
                    controls_thermal = False
                    apply_thermal_state()
                except Exception:
                    traceback.print_exc()
                return

            stop_streaming("gst-error")
            return

        if mtype == Gst.MessageType.WARNING:
            err, dbg = msg.parse_warning()
            print("[GST][WARNING] from", src, ":", err, "| dbg:", dbg)
            return

        if mtype == Gst.MessageType.STATE_CHANGED:
            if msg.src == pipeline:
                 old, new, pending = msg.parse_state_changed()
                 print(f"[GST][PIPE] state {old.value_nick} -> {new.value_nick} (pending {pending.value_nick})")
            return

    bus.connect("message", on_bus_message)

    print("[GST] pipeline construida")
    apply_thermal_state()


def cleanup_firebase_session():
    #  Limpia solo sesión WebRTC (no toca controls ni clientNonce)
    for ref in (offer_ref, answer_ref, caller_cand_ref, callee_cand_ref):
        try:
            ref.delete()
        except Exception:
            pass


def start_streaming(reason=""):
    global pipeline
    global OFFER_IN_FLIGHT, ANSWER_SET, LOCAL_OFFER_PUBLISHED
    global _last_offer_ms, LAST_NONCE, _last_nonce_ms
    global CURRENT_OFFER_ID, LAST_ANSWER_SDP
    global streaming_active

    cleanup_firebase_session()
 
    CURRENT_OFFER_ID = None
    LAST_ANSWER_SDP = None
    OFFER_IN_FLIGHT = False
    ANSWER_SET = False
    LOCAL_OFFER_PUBLISHED = False
    _last_offer_ms = 0
    LAST_NONCE = None
    _last_nonce_ms = 0

    if pipeline is None:
        build_pipeline()

    streaming_active = True # Variable global para controlar el bucle
    pipeline.set_state(Gst.State.PLAYING)
    print("[STREAM] PLAYING. reason:", reason)

    request_offer("start:" + reason)


def stop_streaming(reason=""):
    global pipeline, webrtc, video_encoder
    global sel_video, valve_cam, valve_therm
    global rtpjbuf_rx, rtpopusdep, opusdec, aconv_rx, ares_rx, asink_rx
    global video_parser, video_payloader
    global OFFER_IN_FLIGHT, ANSWER_SET, LOCAL_OFFER_PUBLISHED
    global streaming_active

    print("[STREAM] STOP (NULL). reason:", reason)
    streaming_active = False
    OFFER_IN_FLIGHT = False
    ANSWER_SET = False
    LOCAL_OFFER_PUBLISHED = False

    try:
        if pipeline:
            pipeline.get_bus().set_flushing(True) # Mata cualquier mensaje residual
            pipeline.set_state(Gst.State.NULL)
    except Exception:
        traceback.print_exc()

    # Romper referencias
    pipeline = None
    webrtc = None
    video_encoder = None
    video_parser = None
    video_payloader = None
    sel_video = None
    valve_cam = None
    valve_therm = None
    rtpjbuf_rx = None
    rtpopusdep = None
    opusdec = None
    aconv_rx = None
    ares_rx = None
    asink_rx = None

    cleanup_firebase_session()
    
    # Forzar recolección de basura para liberar memoria y cerrar puertos
    gc.collect() 
    print("[STREAM] Cleanup completo. Puertos y cámara liberados.")



# ===================== Firebase listeners =====================
def listen_firebase():
    global controls_active, controls_thermal

    # No se tocan controls ni clientNonce
    cleanup_firebase_session()

    def active_listener(event):
        global controls_active
        val = event.data
        if isinstance(val, dict):
            val = val.get("data", val)
        enabled = parse_bool(val)

        if enabled == controls_active:
            return

        controls_active = enabled
        print(f"[CTRL] active={controls_active}")

        if controls_active:
            print("[STREAM] Dando 1.2s a Android para limpiar su caché...")
            cleanup_firebase_session() # Borramos todo rastro viejo
            
            # Función anidada para verificar que el usuario no haya salido rápido
            def _delayed_start():
                if controls_active:
                    gst_call(start_streaming, "active=true")
                return False
            
            # Se retrasa el inicio de GStreamer
            GLib.timeout_add(1200, _delayed_start)
        else:
            gst_call(stop_streaming, "active=false")


    def thermal_listener(event):
        global controls_thermal
        val = event.data
        if isinstance(val, dict):
            val = val.get("data", val)
        controls_thermal = parse_bool(val)
        print(f"[CTRL] thermal={controls_thermal}")
        gst_call(apply_thermal_state)

    def answer_listener(event):
        global ANSWER_SET, OFFER_IN_FLIGHT, LAST_ANSWER_SDP
        payload = event.data
        if not payload:
            return
        if time.process_time() < 2.0 and not LOCAL_OFFER_PUBLISHED: 
            print("[signal] Limpiando answer residual de Firebase...") 
            return 
        #Prueba de basura existente

        sdp_text = None
        answer_offer_id = None

        if isinstance(payload, dict):
            sdp_text = payload.get("sdp") or (payload.get("data", {}) or {}).get("sdp")
            answer_offer_id = payload.get("offerId") or (payload.get("data", {}) or {}).get("offerId")

        if not sdp_text:
            return

        
        # Si todavía no se ha publicado una offer, es answer viejo/snapshot -> ignora
        if not LOCAL_OFFER_PUBLISHED:
            print("[signal] ANSWER recibida antes de publicar OFFER -> IGNORADA (stale)")
            return

        # Si se aplicó answer y es el mismo SDP -> ignora duplicado
        if ANSWER_SET and LAST_ANSWER_SDP == sdp_text:
            print("[signal] ANSWER duplicada -> IGNORADA")
            return

        # Si se está usando offerId, valida que corresponda a esta offer
        if (answer_offer_id is not None) and (CURRENT_OFFER_ID is not None) and (answer_offer_id != CURRENT_OFFER_ID):
            print(f"[signal] ANSWER con offerId={answer_offer_id} pero current={CURRENT_OFFER_ID} -> IGNORADA")
            return

        def _apply_answer():
            global ANSWER_SET, OFFER_IN_FLIGHT, LAST_ANSWER_SDP
            if webrtc is None:
                return
            print("[Firebase] ANSWER -> set-remote-description")
            try:
                sdpmsg = sdp_from_text(sdp_text)
                ans = GstWebRTC.WebRTCSessionDescription.new(GstWebRTC.WebRTCSDPType.ANSWER, sdpmsg)
                webrtc.emit("set-remote-description", ans, Gst.Promise.new())

                ANSWER_SET = True
                OFFER_IN_FLIGHT = False
                LAST_ANSWER_SDP = sdp_text

                print("[signal] ANSWER aplicada -> ANSWER_SET=True")
            except Exception:
                traceback.print_exc()

        gst_call(_apply_answer)


    def callee_cand_listener(event):
        """
        Escucha calleeCandidates y SOLO procesa candidatos reales.
        Además, aplica gating: no mete ICE hasta que la OFFER local esté publicada.
        """
        payload = event.data
        if not payload:
            return

        if not LOCAL_OFFER_PUBLISHED:
            return

        def accept_offer_id(cand_offer_id):
            if cand_offer_id is None or CURRENT_OFFER_ID is None:
                return True
            return cand_offer_id == CURRENT_OFFER_ID

 
        def _add_candidate(mline, cand):
            if webrtc is None:
                print("[ICE][REMOTE][SKIP] webrtc=None")
                return
            try:
                print(f"[ICE][REMOTE][ADD] mline={mline} cand={cand[:110]}...")
                webrtc.emit("add-ice-candidate", int(mline), cand)
                print("[ICE][REMOTE][OK] add-ice-candidate emit")
              
                    
            except Exception:
                traceback.print_exc()

        
	# Caso 1: push directo con candidate
        if isinstance(payload, dict) and "candidate" in payload:
            cand = payload.get("candidate")
            if not cand:
                return
            cand_offer_id = payload.get("offerId")
            if not accept_offer_id(cand_offer_id):
                return
            mline = payload.get("sdpMLineIndex", 0)
            gst_call(_add_candidate, mline, cand)
            return

	# Caso 2: snapshot grande
        if isinstance(payload, dict):
            for _, c in payload.items():
                if isinstance(c, dict) and "candidate" in c:
                    cand = c.get("candidate")
                    if not cand:
                        continue
                    cand_offer_id = c.get("offerId")
                    if not accept_offer_id(cand_offer_id):
                        continue
                    mline = c.get("sdpMLineIndex", 0)
                    gst_call(_add_candidate, mline, cand)



    def nonce_listener(event):
        global LAST_NONCE, _last_nonce_ms

        nonce = event.data
        if not nonce:
            return

        print(f"[signal] clientNonce ping recibido: {nonce}")

        if nonce == LAST_NONCE:
            return

        LAST_NONCE = nonce

        if controls_active and streaming_active:
            print("[signal] Android reconectó (nuevo nonce). ¡Reiniciando pipeline con retardo!")
            gst_call(stop_streaming, "reconnect")
            cleanup_firebase_session()
            
            def _delayed_reconnect():
                if controls_active:
                    gst_call(start_streaming, "reconnect")
                return False
            
            GLib.timeout_add(1200, _delayed_reconnect)

    def start_listener(ref, cb, name):
        backoff = 1
        while True:
            try:
                s = ref.listen(cb)
                streams.append(s)
                print(f"[Firebase] listener conectado: {name}")
                return
            except Exception as e:
                print(f"[Firebase][{name}] fallo: {e} (retry {backoff}s)")
                time.sleep(backoff)
                backoff = min(backoff * 2, 20)

    start_listener(controls_active_ref,  active_listener,  "controls.active")
    start_listener(controls_thermal_ref, thermal_listener, "controls.thermal")
    start_listener(answer_ref,          answer_listener,   "answer")
    start_listener(nonce_ref,           nonce_listener,    "clientNonce")
    start_listener(callee_cand_ref, callee_cand_listener, "calleeCandidates")


def close_all():
    try:
        for s in streams:
            try:
                s.close()
            except Exception:
                pass
    except Exception:
        pass

    try:
        gst_call(stop_streaming, "exit")
    except Exception:
        pass


def main():
    print("[Jetson] Limpiando base de datos residual (Hard Reset)...")
    try:
        # Esto borra el árbol de 'jetson_camera_001' al iniciar el script
        call_ref.delete()  
    except Exception as e:
        print("[Jetson] Error limpiando:", e)

    print("[Jetson] Esperando controls/active=true para iniciar streaming… RoomID:", ROOM_ID)

    # Manejo de salida limpia
    def _sig(*_):
        print("\n[Jetson] Señal recibida, cerrando…")
        close_all()
        try:
            GLib.MainLoop().quit()
        except Exception:
            pass
        os._exit(0)

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    threading.Thread(target=listen_firebase, daemon=True).start()

    loop = GLib.MainLoop()
    loop.run()


if __name__ == "__main__":
    main()