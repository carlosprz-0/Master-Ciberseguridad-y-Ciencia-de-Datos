# Criptografía ligera y kleptografía en ML-KEM: implementación y evaluación

Este repositorio contiene la implementación desarrollada para el Trabajo Fin de Máster, centrada en el estudio experimental de una variante kleptográfica aplicada a Kyber/ML-KEM.

El objetivo principal del proyecto es analizar la viabilidad de proteger y recuperar la semilla utilizada durante la generación de claves mediante mecanismos de cifrado autenticado. Para ello, se comparan dos primitivas simétricas:

- **ASCON-AEAD128**
- **AES-CCM128**

La evaluación se realiza sobre los tres niveles de seguridad de Kyber/ML-KEM:

- ML-KEM-512
- ML-KEM-768
- ML-KEM-1024

Además, se comparan los resultados en dos plataformas:

- Ordenador personal
- Raspberry Pi 4

---

## Descripción general

El proyecto parte de una implementación de Kyber/ML-KEM con una modificación experimental en el proceso de generación de claves. Dicha modificación permite proteger una semilla interna y recuperarla posteriormente para reconstruir la clave secreta.

En el diseño original, este tipo de flujo podía apoyarse en mecanismos asimétricos de encapsulamiento de claves. En este trabajo se han integrado dos primitivas AEAD simétricas, ASCON-AEAD128 y AES-CCM128, con el objetivo de comparar su comportamiento temporal y de uso de recursos.

Es importante destacar que ASCON-AEAD128 y AES-CCM128 no son mecanismos KEM, sino primitivas de cifrado autenticado. Por tanto, en esta implementación se utilizan para cifrar y autenticar la semilla asociada al proceso de generación de claves.

---

## Objetivos del repositorio

Los principales objetivos de esta implementación son:

- Integrar ASCON-AEAD128 en el flujo de protección de la semilla.
- Integrar AES-CCM128 como variante de comparación.
- Evaluar la recuperación correcta de la clave secreta.
- Comparar el rendimiento en ML-KEM-512, ML-KEM-768 y ML-KEM-1024.
- Medir tiempos de ejecución por iteración.
- Obtener métricas por fases: inicialización, generación de clave con puerta trasera y recuperación.
- Comparar el comportamiento en ordenador personal y Raspberry Pi 4.
- Realizar un contraste estadístico entre ASCON-AEAD128 y AES-CCM128 mediante Mann-Whitney U, corrección Holm-Bonferroni y Cliff's delta.
