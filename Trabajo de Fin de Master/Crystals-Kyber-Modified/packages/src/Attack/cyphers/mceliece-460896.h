/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date September 29 2024
 * @version v0.1.0
 * @brief This file contains the declaration of the methods of the mceliece--460896 cypher class
 */

#pragma once

#include <iostream>
#include <stdexcept>

#include "./cypher.h"

class McEliece_460896 : public Cypher {
 public:
  McEliece_460896();
  ~McEliece_460896() { OQS_KEM_free(kem); }
  std::pair<Bytes, Bytes> Encrypt(const Bytes& message) override;
  Bytes Decrypt(const Bytes& cyphertext, const Bytes& sk = Bytes()) override;

  McEliece_460896(const McEliece_460896&) = delete;
  McEliece_460896& operator=(const McEliece_460896&) = delete;

 private:
  OQS_KEM* kem;
};