#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Servidor MJPEG del AMG8833 (Grid-EYE 8x8) para Jetson.
# - Dependencias mínimas: smbus, numpy, Flask
# - Opcionales para mejor color/velocidad: opencv-python (cv2) o matplotlib
#
# Uso:
#   python3 thermal_amg8833_mjpeg.py --addr 0x69 --fps 10 --width 320 --height 240 --auto
#
# Endpoint:
#   http://127.0.0.1:5000/thermal.mjpg

import argparse, time, io, threading
from collections import deque

import numpy as np
import smbus

try:
    from flask import Flask, Response
except Exception as e:
    raise SystemExit("Falta Flask. Instala con:  pip3 install --user flask") from e

# Opcionales
try:
    import cv2  # Para colormap rápido y JPEG
    _HAS_CV2 = True
except Exception:
    _HAS_CV2 = False

_HAS_MPL = False
try:
    # En el caso de no tener cv2 se usa matplotlib para colormap
    if not _HAS_CV2:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.cm as cm
        _MPL_CMAP = cm.get_cmap("inferno")
        _HAS_MPL = True
except Exception:
    _HAS_MPL = False

try:
    from PIL import Image  # Ayuda a codificar JPEG si no hay cv2
    _HAS_PIL = True
except Exception:
    _HAS_PIL = False

BUS_ID = 1
THERM_ADDR_DEFAULT = 0x69

THERMISTOR_REG = 0x0E
PIXELS_START   = 0x80
PIXELS_BYTES   = 64 * 2   # 64 pixeles * 2 bytes
BLOCK_LEN      = 32

# Suavizado de vmin/vmax automático
EMA_ALPHA = 0.2

app = Flask(__name__)

def s12(v):
    """Convierte a entero con signo de 12 bits."""
    return v - (1 << 12) if (v & 0x800) else v

class AMG883Server:
    def __init__(self, bus_id, addr, width, height, fps, auto_scale, fix_vmin, fix_vmax):
        self.bus   = smbus.SMBus(bus_id)
        self.addr  = addr
        self.w     = int(width)
        self.h     = int(height)
        self.fps   = float(fps)
        self.auto  = bool(auto_scale)
        self.fix_v = (fix_vmin, fix_vmax)
        self._stop = False

        self._vmin = 25.0
        self._vmax = 32.0

        self._last_jpeg = b""
        self._lock = threading.Lock()

        # Para FPS real
        self._period = 1.0 / max(1e-3, self.fps)

    def read_thermistor_c(self):
        raw = self.bus.read_i2c_block_data(self.addr, THERMISTOR_REG, 2)
        v = s12(raw[0] | (raw[1] << 8))
        return v * 0.0625

    def read_pixels_c(self):
        raw = []
        for off in range(0, PIXELS_BYTES, BLOCK_LEN):
            raw += self.bus.read_i2c_block_data(self.addr, PIXELS_START + off, BLOCK_LEN)
        vals = []
        for i in range(0, PIXELS_BYTES, 2):
            v = s12(raw[i] | (raw[i + 1] << 8))
            vals.append(v * 0.25)
        a = np.array(vals, dtype=np.float32).reshape(8, 8)
        a = np.flipud(a)  # invertir vertical para visual "natural"
        return a

    def _normalize_to_u8(self, a8x8):
        
        # Determinar rango
        if self.auto:
            p_lo, p_hi = np.percentile(a8x8, [10, 90])
            vmin = (1.0 - EMA_ALPHA) * self._vmin + EMA_ALPHA * float(p_lo)
            vmax = (1.0 - EMA_ALPHA) * self._vmax + EMA_ALPHA * float(p_hi)
            if vmax <= vmin:
                vmax = vmin + 0.5
            self._vmin, self._vmax = vmin, vmax
        else:
            vmin, vmax = self.fix_v

        # Normalizar 0..255
        span = max(1e-6, vmax - vmin)
        norm = (a8x8 - vmin) / span
        norm = np.clip(norm, 0.0, 1.0)
        u8 = (norm * 255.0).astype(np.uint8)
        return u8

    def _resize_smooth(self, small_u8):
        # Se calcula el tamaño del cuadrado perfecto basado en la altura (360x360)
        square_size = self.h

        if _HAS_CV2:
            # Interpolación bicúbica para lograr el degradado suave 
            smooth_square = cv2.resize(small_u8, (square_size, square_size), interpolation=cv2.INTER_CUBIC)
        elif _HAS_PIL:
            im = Image.fromarray(small_u8, mode="L")
            im = im.resize((square_size, square_size), resample=Image.BICUBIC)
            smooth_square = np.array(im, dtype=np.uint8)
        else:
            smooth_square = np.kron(small_u8, np.ones((square_size // 8, square_size // 8), dtype=np.uint8))

        # Se crea un lienzo negro panorámico panorámico 
        canvas = np.zeros((self.h, self.w), dtype=np.uint8)

        # Se pega la imagen térmica suave justo en el centro del lienzo
        x_offset = (self.w - square_size) // 2
        canvas[:, x_offset:x_offset+square_size] = smooth_square

        return canvas


    def _colorize(self, gray_u8):
        if _HAS_CV2:
            colored = cv2.applyColorMap(gray_u8, cv2.COLORMAP_INFERNO)
            # BGR -> RGB
            colored = cv2.cvtColor(colored, cv2.COLOR_BGR2RGB)
            return colored

        if _HAS_MPL:
            norm = (gray_u8.astype(np.float32) / 255.0)
            rgb = (_MPL_CMAP(norm)[..., :3] * 255.0).astype(np.uint8)
            return rgb

        return np.dstack([gray_u8, gray_u8, gray_u8])

    def _encode_jpeg(self, rgb):
        if _HAS_CV2:
            ok, buf = cv2.imencode(".jpg", cv2.cvtColor(rgb, cv2.COLOR_RGB2BGR), [int(cv2.IMWRITE_JPEG_QUALITY), 85])
            if not ok:
                return b""
            return buf.tobytes()

        if _HAS_PIL:
            im = Image.fromarray(rgb, mode="RGB")
            bio = io.BytesIO()
            im.save(bio, format="JPEG", quality=85)
            return bio.getvalue()

        # Si no hay codificador JPEG disponible:
        raise RuntimeError(
            "No hay codificador JPEG disponible. Instala uno:\n"
            " - Opción A (recomendada):  pip3 install --user opencv-python\n"
            " - Opción B:                pip3 install --user pillow"
        )

    def make_frame(self):
        a = self.read_pixels_c()             # (8x8) float32 °C
        gray = self._normalize_to_u8(a)      # (8x8) uint8
        big  = self._resize_smooth(gray)     
        rgb  = self._colorize(big)           # (H x W x 3) uint8
        jpg  = self._encode_jpeg(rgb)        # bytes JPEG
        return jpg


    def producer_loop(self):
        next_t = time.time()
        while not self._stop:
            try:
                jpg = self.make_frame()
                with self._lock:
                    self._last_jpeg = jpg
            except Exception as e:
                # Evita matar el hilo por un fallo momentáneo de I2C y reintenta
                print("[THERM] error en captura:", e)
            # Ritmo de FPS
            next_t += self._period
            delay = next_t - time.time()
            if delay > 0:
                time.sleep(delay)
            else:
                next_t = time.time()

    def get_last_jpeg(self):
        with self._lock:
            return self._last_jpeg

    def stop(self):
        self._stop = True

# ----------- App / Rutas -----------
_instance = None

@app.route("/")
def index():
    return (
        "<html><body>"
        "<h3>AMG8833 MJPEG</h3>"
        '<img src="/thermal.mjpg" />'
        "</body></html>"
    )

@app.route("/thermal.mjpg")
def mjpeg():
    def gen():
        boundary = b"--frame"
        while True:
            frame = _instance.get_last_jpeg()
            if not frame:
                # Si aún no hay cuadro, espera un poco
                time.sleep(0.05)
                continue
            yield (boundary + b"\r\n"
                   b"Content-Type: image/jpeg\r\n"
                   b"Content-Length: " + str(len(frame)).encode("ascii") + b"\r\n\r\n" +
                   frame + b"\r\n")
    return Response(gen(),
                    mimetype="multipart/x-mixed-replace; boundary=frame")

def parse_args():
    ap = argparse.ArgumentParser(description="Servidor MJPEG para AMG8833 (Grid-EYE)")
    ap.add_argument("--bus", type=int, default=BUS_ID, help="I2C bus (default 1)")
    ap.add_argument("--addr", type=lambda x: int(x, 0), default=THERM_ADDR_DEFAULT, help="Dirección I2C (default 0x69)")
    ap.add_argument("--width", type=int, default=640, help="Ancho de la imagen")
    ap.add_argument("--height", type=int, default=360, help="Alto de la imagen")
    ap.add_argument("--fps", type=float, default=10.0, help="FPS")
    ap.add_argument("--auto", action="store_true", help="Auto-ajuste de rango (percentiles + EMA)")
    ap.add_argument("--vmin", type=float, default=25.0, help="Rango fijo - vmin (si no usas --auto)")
    ap.add_argument("--vmax", type=float, default=32.0, help="Rango fijo - vmax (si no usas --auto)")
    ap.add_argument("--host", default="127.0.0.1", help="Host para el servidor HTTP")
    ap.add_argument("--port", type=int, default=5000, help="Puerto HTTP")
    return ap.parse_args()

def main():
    global _instance
    args = parse_args()

    if not _HAS_CV2 and not _HAS_PIL:
        print("[WARN] No hay cv2 ni Pillow; necesito al menos uno para JPEG.")
        print("Instala uno:")
        print("  pip3 install --user opencv-python")
        print("    (o) pip3 install --user pillow")
        # seguimos, pero fallará al codificar JPEG

    _instance = AMG883Server(
        bus_id=args.bus,
        addr=args.addr,
        width=args.width,
        height=args.height,
        fps=args.fps,
        auto_scale=args.auto,
        fix_vmin=args.vmin,
        fix_vmax=args.vmax,
    )

    th = threading.Thread(target=_instance.producer_loop, daemon=True)
    th.start()

    print(f"[THERM] servidor en http://{args.host}:{args.port}/thermal.mjpg  (FPS={args.fps})")
    try:
        # threaded=True permite atender varias conexiones al MJPEG
        app.run(host=args.host, port=args.port, threaded=True, debug=False)
    except KeyboardInterrupt:
        pass
    finally:
        _instance.stop()

if __name__ == "__main__":
    main()