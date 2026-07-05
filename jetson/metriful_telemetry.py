#!/usr/bin/env python3
# Jetson <-> Firestore (Admin SDK) + Metriful (I2C) + Arduino (USB Serial)
# - Rollover local (dayKey) a la hora indicada
# - Auto-simulacion si no hay sensor conectado
# - Cambio dinamico entre simulacion y lectura real
# - Listener de acciones con ACK (la app hace el reset)
# - Reproduccion de MP3 sincronizada con tira LED via Arduino
# - Bloqueo dinamico de luz utilitaria durante rutinas

import os
import time
import threading
import random
import subprocess
import shutil
import serial
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from google.cloud import firestore
from google.oauth2 import service_account
import smbus

load_dotenv()

# ========= CONFIG =========
SERVICE_ACCOUNT = os.getenv("FIREBASE_SERVICE_ACCOUNT")
if not SERVICE_ACCOUNT or not os.path.isfile(SERVICE_ACCOUNT):
    raise SystemExit("ERROR: Credenciales de Firebase no encontradas. Revisa tu archivo .env")

ROOM_ID         = os.getenv("ROOM_ID", "default_room")
BUS_ID          = int(os.getenv("I2C_BUS_ID", "1"))
ADDR            = 0x71
SIMULATE_FORCED = os.getenv("SIMULATE", "0") == "1"
MEASURE_INTERVAL= int(os.getenv("MEASURE_INTERVAL", "60"))
ROLLOVER_HOUR   = int(os.getenv("ROLLOVER_HOUR", "5"))

MUSIC_DIR       = os.getenv("MUSIC_DIR", "./Music")

# ========= CONEXION SERIAL CON ARDUINO =========
SERIAL_PORT     = os.getenv("SERIAL_PORT", "/dev/ttyUSB0")
SERIAL_BAUD     = 9600

# ========= CONFIGURACION DE TIEMPOS (SEGUNDOS) =========
AUDIO_DURATIONS = {
    "whiteNoise": 55,    
    "music": 157,        
    "dayRoutine": 127,   
    "nightRoutine": 225, 
}

print(f"Conectando con Arduino en {SERIAL_PORT}...")
try:
    arduino = serial.Serial(SERIAL_PORT, SERIAL_BAUD, timeout=1)
    time.sleep(2)
    print("Arduino conectado exitosamente.")
except Exception as e:
    arduino = None
    print(f"No se pudo conectar al Arduino: {e}")

def send_arduino_command(cmd_char: str, duration_sec: int = 0):
    if arduino and arduino.is_open:
        msg = f"{cmd_char},{duration_sec}\n"
        arduino.write(msg.encode('utf-8'))
        # Se comenta el print de Arduino para no ensuciar los logs de los sensores
        # print(f"Enviado a Arduino: {msg.strip()}")

# ========= Firestore Admin =========
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT)
db    = firestore.Client(credentials=creds, project=creds.project_id)


# ========= Utilidades =========
def now_ms() -> int: return int(time.time() * 1000)
def iso_now() -> str: return datetime.now(timezone.utc).isoformat()
def day_key() -> str:
    now = datetime.now().astimezone()
    effective = now - timedelta(days=1) if now.hour < ROLLOVER_HOUR else now
    return effective.strftime("%Y-%m-%d")
def _which(cmd: str) -> bool: return shutil.which(cmd) is not None

def play_mp3_blocking(file_path: str) -> bool:
    if not os.path.isfile(file_path):
        print(f"No existe el archivo de audio: {file_path}")
        return False
    if _which("mpg123"): subprocess.run(["mpg123", "-q", file_path], check=False); return True
    if _which("ffplay"): subprocess.run(["ffplay", "-nodisp", "-autoexit", "-loglevel", "quiet", file_path], check=False); return True
    if _which("cvlc"): subprocess.run(["cvlc", "--play-and-exit", "--quiet", file_path], check=False); return True
    print("No encontre reproductor (mpg123/ffplay/cvlc).")
    return False

# ========= Decodificadores Metriful =========
def extract_air(data):
    if len(data) != 12: raise ValueError("AIR_DATA bytes incorrectos")
    t_int_frac = data[0]
    t_c = (t_int_frac & 0x7F) + (data[1] / 10.0)
    if (t_int_frac & 0x80) != 0: t_c = -t_c
    h_pc = data[6] + (data[7] / 10.0)
    return t_c, h_pc

def extract_aq(data):
    if len(data) != 10: raise ValueError("AQ_DATA bytes incorrectos")
    return data[3] + (data[4] << 8) + (data[5] / 10.0)

def extract_sound(data):
    if len(data) != 18: raise ValueError("SOUND bytes incorrectos")
    return data[0] + (data[1] / 10.0)

# ========= Warnings =========
LIMITS = {"temperature": (18.0, 27.0, "°C"), "humidity": (30.0, 60.0, "%"), "co2": (None, 1200.0, "ppm"), "noise": (None, 55.0, "dBA")}
KEY_TO_KIND = {"temperature": "TEMP", "humidity": "HUMIDITY", "co2": "CO2", "noise": "NOISE"}
TIPS = {"temperature": "Revisa ventilacion o ropa.", "humidity": "Ventila el cuarto.", "co2": "Ventila.", "noise": "Reduce ruidos."}

def check_warnings(meas: dict):
    out = []
    for key, value in meas.items():
        if key not in LIMITS: continue
        low, high, unit = LIMITS[key]
        over  = (high is not None and value > high)
        under = (low  is not None and value < low)
        if over or under:
            out.append({"kind": KEY_TO_KIND[key], "value": float(value), "unit": unit, "threshold": float(high if over else low), "message": TIPS.get(key), "createdAt": firestore.SERVER_TIMESTAMP, "createdAtLocal": now_ms()})
    return out

# ========= Firestore I/O =========
def get_owner_uid(room_id: str) -> str:
    doc = db.collection("devices").document(room_id).get()
    if not doc.exists: raise RuntimeError("devices/%s no existe" % room_id)
    return doc.to_dict().get("ownerUid")

def write_measurements(uid: str, meas: dict):
    ref = db.collection("users").document(uid).collection("measurements").document("current")
    body = {"temperature": float(meas.get("temperature", 0.0)), "humidity": float(meas.get("humidity", 0.0)), "co2": float(meas.get("co2", 0.0)), "noise": float(meas.get("noise", 0.0)), "updatedAt": firestore.SERVER_TIMESTAMP, "updatedAtLocal": now_ms()}
    ref.set(body, merge=True)
    print("Measurements actualizados:", body)

def append_warning(uid: str, w: dict):
    ref = db.collection("users").document(uid).collection("daily").document(day_key()).collection("warnings").document()
    ref.set(w, merge=False)
    print(f"Warning enviado: {w['kind']}={w['value']}{w['unit']} (>{w['threshold']})")

# ========= Acciones: musica + luz =========
def actions_watchdog(uid: str, stop_event: threading.Event):
    doc_ref = db.collection("users").document(uid).collection("actions").document("current")
    print("Escuchando acciones...")
    
    CANON = {"whiteNoise": "onWhiteNoise.mp3", "music": "onMusic.mp3", "dayRoutine": "onDayRoutine.mp3", "nightRoutine": "onNightRoutine.mp3"}
    
    def read_flag(data, key):
        if key == "dayRoutine": return bool(data.get("dayRoutine") or data.get("dayRutine"))
        if key == "nightRoutine": return bool(data.get("nightRoutine") or data.get("nightRutine"))
        return bool(data.get(key))

    last_flag = {k: None for k in CANON.keys()}
    last_flag["light"] = False # Se inicializa la memoria de la luz
    last_ack_written = {}
    playing = set()
    lock = threading.Lock()

    def run_audio_action(key: str, filename: str):
        try:
            duration = AUDIO_DURATIONS.get(key, 60)
            if key == "whiteNoise":   send_arduino_command('W', duration)
            elif key == "music":      send_arduino_command('M', duration)
            elif key == "dayRoutine": send_arduino_command('D', duration)
            elif key == "nightRoutine": send_arduino_command('N', duration)

            path = os.path.join(MUSIC_DIR, filename)
            play_mp3_blocking(path)
        finally:
            # Al terminar el audio, se regresa al estado que tenia la luz
            if last_flag.get("light"):
                send_arduino_command('L', 0)
            else:
                send_arduino_command('O', 0)
                
            with lock:
                playing.discard(key)

    def on_snapshot(doc_snapshot, changes, read_time):
        if not doc_snapshot: return
        data = doc_snapshot[0].to_dict() or {}

        # --- LOGICA DE BLOQUEO DE LUZ ---
        if "light" in data:
            desired = bool(data.get("light"))
            if desired != last_flag.get("light"):
                
                with lock:
                    is_playing = len(playing) > 0
                
                if is_playing:
                    print("Rutina en progreso. Bloqueando cambio de luz y revirtiendo UI.")
                    # Se revierte el cambio en Firestore para que la app movil se corrija
                    doc_ref.set({"light": last_flag.get("light")}, merge=True)
                else:
                    # Si no hay rutina sonando, operamos normal
                    send_arduino_command('L', 0) if desired else send_arduino_command('O', 0)
                    last_flag["light"] = desired
            
            # Siempre mandar el ACK
            ack_field = "lightAckAt"
            with lock: last = last_ack_written.get("light")
            if data.get(ack_field) != last:
                ack_now = iso_now()
                doc_ref.set({ack_field: ack_now}, merge=True)
                with lock: last_ack_written["light"] = ack_now

        # --- LOGICA DE AUDIOS ---
        for key, fname in CANON.items():
            flag = read_flag(data, key)
            prev_flag = last_flag.get(key)
            rising = (flag is True and prev_flag is not True)
            ack_field = f"{key}AckAt"
            
            with lock:
                last_ack = last_ack_written.get(key)
                already = (key in playing)

            if not rising and data.get(ack_field) == last_ack: continue

            if flag:
                with lock:
                    if already: continue
                    playing.add(key)
                ack_now = iso_now()
                doc_ref.set({ack_field: ack_now}, merge=True)
                with lock: last_ack_written[key] = ack_now
                threading.Thread(target=run_audio_action, args=(key, fname), daemon=True).start()

            last_flag[key] = flag

    watch = doc_ref.on_snapshot(on_snapshot)
    try:
        while not stop_event.is_set(): time.sleep(0.2)
    finally:
        watch.unsubscribe()
        print("Accion listener detenido.")

# ========= I2C / Metriful =========
def probe_metriful(bus) -> bool:
    try:
        bus.write_byte(ADDR, 0xE1)
        time.sleep(0.5)
        bus.read_i2c_block_data(ADDR, 0x10, 12)
        return True
    except OSError:
        return False

def read_measurements(bus, allow_simulate: bool):
    try:
        bus.write_byte(ADDR, 0xE1)
        time.sleep(1.0)
        air_raw   = bus.read_i2c_block_data(ADDR, 0x10, 12)
        aq_raw    = bus.read_i2c_block_data(ADDR, 0x11, 10)
        sound_raw = bus.read_i2c_block_data(ADDR, 0x13, 18)

        t_c, h_pc = extract_air(air_raw)
        meas = {
            "temperature": round(t_c, 1),
            "humidity":    round(h_pc, 1),
            "co2":         int(round(extract_aq(aq_raw), 0)),
            "noise":       round(extract_sound(sound_raw), 1),
        }
        return meas
    except OSError:
        if allow_simulate:
            base_t = random.uniform(20.0, 26.0)
            base_h = random.uniform(35.0, 55.0)
            base_c = random.uniform(500, 1400)
            base_n = random.uniform(35.0, 65.0)
            meas = {
                "temperature": round(base_t, 1),
                "humidity":    round(base_h, 1),
                "co2":         int(base_c),
                "noise":       round(base_n, 1),
            }
            print("SIM MODE ->", meas)
            return meas
        else:
            raise

# ========= MAIN =========
def main():
    print("Iniciando Jetson con ROOM_ID=%s" % ROOM_ID)
    uid = get_owner_uid(ROOM_ID)
    print("Dueno actual: uid=%s" % uid)

    stop_event = threading.Event()
    t = threading.Thread(target=actions_watchdog, args=(uid, stop_event), daemon=True)
    t.start()

    bus = smbus.SMBus(BUS_ID)

    if SIMULATE_FORCED:
        sim_mode = True
        print("Modo simulacion FORZADO (SIMULATE=1).")
    else:
        sim_mode = not probe_metriful(bus)
        if sim_mode:
            print("Metriful no detectado. Entrando en SIMULACION hasta que se conecte...")
        else:
            print("Metriful detectado. Modo REAL.")

    print(f"Midiendo cada {MEASURE_INTERVAL}s... (Ctrl+C para salir)")
    probe_counter = 0

    try:
        while True:
            try:
                meas = read_measurements(bus, allow_simulate=sim_mode)
                write_measurements(uid, meas)
                for w in check_warnings(meas):
                    append_warning(uid, w)

                if sim_mode and not SIMULATE_FORCED:
                    probe_counter += 1
                    if probe_counter >= 5:
                        probe_counter = 0
                        if probe_metriful(bus):
                            sim_mode = False
                            print("Sensor detectado: cambio a MODO REAL.")

            except OSError as e:
                print("I2C error:", e)
                if not SIMULATE_FORCED:
                    sim_mode = True
                    print("Cambio a MODO SIMULACION. Reintentare sensor periodicamente.")
                else:
                    print("SIMULATE_FORCED activo; reintentando en 5s.")
                    time.sleep(5)

            time.sleep(MEASURE_INTERVAL)

    except KeyboardInterrupt:
        print("\nSaliendo...")
    finally:
        stop_event.set()
        t.join(timeout=2)
        print("Accion listener detenido.")

if __name__ == "__main__":
    main()