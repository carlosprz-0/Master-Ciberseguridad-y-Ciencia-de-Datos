#pragma once

#include <array>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include "./cypher.h"

// Implementación de AES-CCM128 como mecanismo AEAD alternativo.
// Se añade como comparativa frente a ASCON-AEAD128 en el contexto del ataque.
// Igual que ASCON, AES-CCM cifra y autentica la semilla usada por Kyber.

class AESCCM128 : public Cypher {
 public:
  AESCCM128();

  std::pair<Bytes, Bytes> Encrypt(const Bytes& message) override;
  std::pair<Bytes, Bytes> EncryptWithKey(const Bytes& message, const Bytes& key_material); // Cifra la semilla con AES-CCM128 usando una clave derivada del material secreto.
  Bytes Decrypt(const Bytes& cyphertext, const Bytes& sk = Bytes()) override;

 private:
  static constexpr int KEY_SIZE = 16;
  static constexpr int KEY_MATERIAL_SIZE = 32;
  static constexpr int NONCE_SIZE = 12;
  static constexpr int TAG_SIZE = 16;

  Bytes BuildAssociatedData_() const;
  Bytes GenerateNonce_() const;
  Bytes GenerateKeyMaterial_() const;
  std::array<unsigned char, KEY_SIZE> DeriveKey_(const Bytes& key_material) const; // Deriva una clave AES de 128 bits a partir del material secreto del atacante.
};