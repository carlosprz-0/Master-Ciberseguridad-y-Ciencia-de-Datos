#pragma once

#include <array>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include "./cypher.h"

/*
Quitamos la clave fija key_.
Añadimos KEY_MATERIAL_SIZE = 32, que coincide con el tamaño de la semilla de Kyber.
Añadimos DeriveKey_, que derivará una clave ASCON válida de 16 bytes.
Añadimos EncryptWithKey, para poder cifrar usando el material secreto del atacante.
*/

class AsconAEAD128 : public Cypher {
 public:
  AsconAEAD128();

  std::pair<Bytes, Bytes> Encrypt(const Bytes& message) override;
  std::pair<Bytes, Bytes> EncryptWithKey(const Bytes& message, const Bytes& key_material);
  Bytes Decrypt(const Bytes& cyphertext, const Bytes& sk = Bytes()) override;

 private:
  static constexpr int KEY_SIZE = 16;
  static constexpr int KEY_MATERIAL_SIZE = 32;
  static constexpr int NONCE_SIZE = 16;
  static constexpr int TAG_SIZE = 16;

  Bytes BuildAssociatedData_() const;
  Bytes GenerateNonce_() const;
  Bytes GenerateKeyMaterial_() const;
  std::array<uint8_t, KEY_SIZE> DeriveKey_(const Bytes& key_material) const;
};