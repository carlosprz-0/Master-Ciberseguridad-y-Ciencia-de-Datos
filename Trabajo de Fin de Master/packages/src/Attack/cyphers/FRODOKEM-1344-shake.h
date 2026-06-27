/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date September 29 2024
 * @version v0.1.0
 * @brief This file contains the declaration of the methods of the FRODOKEM-1344-shake cypher class
 */

#pragma once

#include <iostream>
#include <stdexcept>

#include "./cypher.h"

class Frodokem_1344_shake : public Cypher {
 public:
  Frodokem_1344_shake();
  ~Frodokem_1344_shake() { OQS_KEM_free(kem); }
  std::pair<Bytes, Bytes> Encrypt(const Bytes& message) override;
  Bytes Decrypt(const Bytes& cyphertext, const Bytes& sk = Bytes()) override;

  Frodokem_1344_shake(const Frodokem_1344_shake&) = delete;
  Frodokem_1344_shake& operator=(const Frodokem_1344_shake&) = delete;

 private:
  OQS_KEM* kem;
};