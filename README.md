# smart-cradle-nvidia-jetson-nano

Este repositorio contiene el núcleo de procesamiento embebido y firmware desarrollado para el proyecto de tesis de ingeniería mecatrónica: **Cuna Inteligente con enfoque IoT para el monitoreo integral de infantes**.

## Componentes Incluidos:
* **Procesamiento y Servidor Térmico (Python / Jetson Nano):** Algoritmos para la adquisición de datos del sensor infrarrojo matricial AMG8833.
* **Módulo de Comunicación (Python / WebRTC):** Lógica para la transmisión bidireccional de video y audio en tiempo real.
* **Telemetría Ambiental (Python / Firebase):** Monitoreo de variables físicas con el sensor Metriful MS430 y sincronización en la nube.
* **Control de Actuadores (C++ / Arduino):** Firmware para la gestión PWM de la tira LED direccionable WS2812B (iluminación ambiental).
