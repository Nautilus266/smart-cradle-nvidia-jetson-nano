#include <Adafruit_NeoPixel.h>

#define PIN        4 
#define NUMPIXELS  8 

Adafruit_NeoPixel pixels(NUMPIXELS, PIN, NEO_GRB + NEO_KHZ800);

// Variables de la Máquina de Estados
char currentMode = 'O'; // 'O' = Off (Apagado por defecto)
unsigned long effectStartTime = 0;
unsigned long effectDurationMs = 0;
uint16_t rainbowHue = 0;

void setup() {
  Serial.begin(9600); 
  pixels.begin();
  pixels.setBrightness(100); 
  apagar();
}

void loop() {
  checkSerial(); 
  
  switch(currentMode) {
    case 'W': heartbeatMode();  break; // Dispara el latido
    case 'M': musicMode();      break;
    case 'D': dayRoutineMode(); break;
    case 'N': nightRoutineMode(); break;
    case 'L': utilityLight();   break;
    case 'O': apagar();         break;
  }
}

// =====================================
// COMUNICACIÓN CON JETSON
// =====================================

void checkSerial() {
  if (Serial.available() > 0) {
    String input = Serial.readStringUntil('\n');
    if (input.length() >= 1) {
      currentMode = input.charAt(0); 
      
      if (input.indexOf(',') != -1) {
        String timeStr = input.substring(input.indexOf(',') + 1);
        unsigned long seconds = timeStr.toInt();
        effectDurationMs = seconds * 1000UL; 
      }
      
      effectStartTime = millis(); 
      pixels.clear(); 
    }
  }
}

// =====================================
// EFECTOS VISUALES
// =====================================

// 1. Latido de Corazón (Para "White Noise")
void heartbeatMode() {
  // Un ciclo completo de latido dura 1000ms (1 segundo)
  unsigned long cycleTime = (millis() - effectStartTime) % 1000; 
  float intensity = 0.0;

  // Primer latido (0 a 150ms)
  if (cycleTime < 150) {
    intensity = sin((cycleTime / 150.0) * PI);
  } 
  // Segundo latido (250 a 400ms)
  else if (cycleTime > 250 && cycleTime < 400) {
    intensity = sin(((cycleTime - 250) / 150.0) * PI);
  }

  // Multiplicamos la intensidad para lograr un rojo/naranja cálido y profundo
  int r = 255 * intensity;
  int g = 20 * intensity;  
  int b = 0;

  fillColor(pixels.Color(r, g, b));
}

// 2 -> Luz Utilitaria: Blanco Frío brillante
void utilityLight() {
  fillColor(pixels.Color(255, 255, 255));
}

// 3 -> Apagar Todo
void apagar() {
  fillColor(pixels.Color(0, 0, 0));
}

// 4 -> Música: Arcoíris suave
void musicMode() {
  static unsigned long lastUpdate = 0;
  if (millis() - lastUpdate > 15) {
    for(int i=0; i<pixels.numPixels(); i++) {
      int pixelHue = rainbowHue + (i * 65536L / pixels.numPixels());
      pixels.setPixelColor(i, pixels.gamma32(pixels.ColorHSV(pixelHue)));
    }
    pixels.show();
    rainbowHue += 256;
    lastUpdate = millis();
  }
}

// 5 -> Rutina de Día: Amanecer (Fade-in Azul)
void dayRoutineMode() {
  unsigned long elapsed = millis() - effectStartTime;
  if (elapsed >= effectDurationMs) {
    fillColor(pixels.Color(0, 0, 255));
    return;
  }
  
  float progress = (float)elapsed / effectDurationMs;
  int blueValue = progress * 255; 
  fillColor(pixels.Color(0, 0, blueValue));
}

// 6 -> Rutina de Noche: Atardecer (Fade-out Rojo)
void nightRoutineMode() {
  unsigned long elapsed = millis() - effectStartTime;
  if (elapsed >= effectDurationMs) {
    fillColor(pixels.Color(0, 0, 0));
    return;
  }
  
  float progress = (float)elapsed / effectDurationMs;
  int redValue = (1.0 - progress) * 255; 
  fillColor(pixels.Color(redValue, 0, 0));
}

void fillColor(uint32_t color) {
  for(int i = 0; i < pixels.numPixels(); i++) {
    pixels.setPixelColor(i, color);
  }
  pixels.show();
}