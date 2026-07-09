#pragma once

#include <array>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include "./cypher.h"

class AESCCM128 : public Cypher {
 public:
  AESCCM128();

  std::pair<Bytes, Bytes> Encrypt(const Bytes& message) override;
  std::pair<Bytes, Bytes> EncryptWithKey(const Bytes& message, const Bytes& key_material);
  Bytes Decrypt(const Bytes& cyphertext, const Bytes& sk = Bytes()) override;

 private:
  static constexpr int KEY_SIZE = 16;
  static constexpr int KEY_MATERIAL_SIZE = 32;
  static constexpr int NONCE_SIZE = 12;
  static constexpr int TAG_SIZE = 16;

  Bytes BuildAssociatedData_() const;
  Bytes GenerateNonce_() const;
  Bytes GenerateKeyMaterial_() const;
  std::array<unsigned char, KEY_SIZE> DeriveKey_(const Bytes& key_material) const;
};