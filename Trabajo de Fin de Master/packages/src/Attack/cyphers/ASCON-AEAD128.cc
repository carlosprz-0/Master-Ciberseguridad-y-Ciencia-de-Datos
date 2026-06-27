#include "./ASCON-AEAD128.h"

#include <cstring>
#include <random>
#include <stdexcept>
#include <vector>

#include "../../Components/Keccak.h"

extern "C" {
  #include "./ascon/api.h"
  #include "./ascon/crypto_aead.h"
}

/*
Ya no existe clave fija: 0x00, 0x01, 0x02, ...

Ahora ASCON genera un material secreto de 32 bytes: static constexpr int KEY_MATERIAL_SIZE = 32;

De ese material se deriva una clave real de 16 bytes: Bytes derived_key = Keccak::KDF(domain_separator + key_material, KEY_SIZE);

Decrypt ahora usa el parámetro sk: Bytes key_material = sk;
*/

AsconAEAD128::AsconAEAD128() {
  Bytes key_material = GenerateKeyMaterial_();

  public_key_ = key_material.GetBytes();
  secret_key_ = key_material.GetBytes();
}

Bytes AsconAEAD128::BuildAssociatedData_() const {
  return Bytes("ASCON-KYBER-SEED-LAB");
}

Bytes AsconAEAD128::GenerateNonce_() const {
  std::vector<uint8_t> nonce(NONCE_SIZE);

  std::random_device rd;
  for (int i = 0; i < NONCE_SIZE; ++i) {
    nonce[i] = static_cast<uint8_t>(rd());
  }

  return Bytes(nonce);
}

Bytes AsconAEAD128::GenerateKeyMaterial_() const {
  std::vector<uint8_t> key_material(KEY_MATERIAL_SIZE);

  std::random_device rd;
  for (int i = 0; i < KEY_MATERIAL_SIZE; ++i) {
    key_material[i] = static_cast<uint8_t>(rd());
  }

  return Bytes(key_material);
}

std::array<uint8_t, AsconAEAD128::KEY_SIZE> AsconAEAD128::DeriveKey_(const Bytes& key_material) const {
  if (key_material.GetBytesSize() == 0) {
    throw std::runtime_error("ERROR: Empty ASCON key material.");
  }

  Bytes domain_separator("ASCON-AEAD128-KEY");
  Bytes derived_key = Keccak::KDF(domain_separator + key_material, KEY_SIZE);
  std::vector<uint8_t> derived_key_vector = derived_key.GetBytes();

  std::array<uint8_t, KEY_SIZE> key{};
  for (int i = 0; i < KEY_SIZE; ++i) {
    key[i] = derived_key_vector[i];
  }

  return key;
}

std::pair<Bytes, Bytes> AsconAEAD128::Encrypt(const Bytes& message) {
  Bytes internal_key_material(secret_key_);
  return EncryptWithKey(message, internal_key_material);
}

std::pair<Bytes, Bytes> AsconAEAD128::EncryptWithKey(const Bytes& message, const Bytes& key_material) {
  Bytes nonce = GenerateNonce_();
  Bytes associated_data = BuildAssociatedData_();
  std::array<uint8_t, KEY_SIZE> key = DeriveKey_(key_material);

  std::vector<uint8_t> plaintext = message.GetBytes();
  std::vector<uint8_t> ad = associated_data.GetBytes();
  std::vector<uint8_t> nonce_vector = nonce.GetBytes();

  std::vector<uint8_t> ciphertext(plaintext.size() + TAG_SIZE);
  unsigned long long ciphertext_len = 0;

  int status = crypto_aead_encrypt(
      ciphertext.data(),
      &ciphertext_len,
      plaintext.data(),
      plaintext.size(),
      ad.data(),
      ad.size(),
      nullptr,
      nonce_vector.data(),
      key.data()
  );

  if (status != 0) {
    throw std::runtime_error("ERROR: Unable to encrypt using ASCON-AEAD128.");
  }

  std::vector<uint8_t> output;
  output.insert(output.end(), nonce_vector.begin(), nonce_vector.end());
  output.insert(output.end(), ciphertext.begin(), ciphertext.begin() + ciphertext_len);

  Bytes cyphertext_bytes(output);

  return {cyphertext_bytes, message};
}

Bytes AsconAEAD128::Decrypt(const Bytes& cyphertext, const Bytes& sk) {
  std::vector<uint8_t> input = cyphertext.GetBytes();

  if (input.size() < NONCE_SIZE + TAG_SIZE) {
    throw std::runtime_error("ERROR: Invalid ASCON ciphertext size.");
  }

  Bytes key_material = sk;
  if (key_material.GetBytesSize() == 0) {
    key_material = Bytes(secret_key_);
  }

  std::array<uint8_t, KEY_SIZE> key = DeriveKey_(key_material);

  std::vector<uint8_t> nonce(
    input.begin(),
    input.begin() + NONCE_SIZE
  );

  std::vector<uint8_t> encrypted_seed(
    input.begin() + NONCE_SIZE,
    input.end()
  );

  Bytes associated_data = BuildAssociatedData_();
  std::vector<uint8_t> ad = associated_data.GetBytes();

  std::vector<uint8_t> plaintext(encrypted_seed.size() - TAG_SIZE);
  unsigned long long plaintext_len = 0;

  int status = crypto_aead_decrypt(
      plaintext.data(),
      &plaintext_len,
      nullptr,
      encrypted_seed.data(),
      encrypted_seed.size(),
      ad.data(),
      ad.size(),
      nonce.data(),
      key.data()
  );

  if (status != 0) {
    throw std::runtime_error("ERROR: ASCON authentication failed.");
  }

  plaintext.resize(plaintext_len);
  return Bytes(plaintext);
}