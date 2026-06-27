#include "./AES-CCM128.h"

#include <openssl/evp.h>

#include <random>
#include <stdexcept>
#include <vector>

#include "../../Components/Keccak.h"

// Inicializa AES-CCM128 generando material secreto de 32 bytes.
// Este material se usa posteriormente para derivar la clave AES efectiva.
AESCCM128::AESCCM128() {
  Bytes key_material = GenerateKeyMaterial_();

  public_key_ = key_material.GetBytes();
  secret_key_ = key_material.GetBytes();
}

Bytes AESCCM128::BuildAssociatedData_() const {
  return Bytes("AES-CCM-KYBER-SEED-LAB");
}

Bytes AESCCM128::GenerateNonce_() const {
  std::vector<unsigned char> nonce(NONCE_SIZE);

  std::random_device rd;
  for (int i = 0; i < NONCE_SIZE; ++i) {
    nonce[i] = static_cast<unsigned char>(rd());
  }

  return Bytes(nonce);
}

// Genera material secreto de tamaño equivalente al de la semilla.
// Posteriormente este material se procesa con una KDF para obtener una clave ASCON válida.
Bytes AESCCM128::GenerateKeyMaterial_() const {
  std::vector<unsigned char> key_material(KEY_MATERIAL_SIZE);

  std::random_device rd;
  for (int i = 0; i < KEY_MATERIAL_SIZE; ++i) {
    key_material[i] = static_cast<unsigned char>(rd());
  }

  return Bytes(key_material);
}

// Aplica una KDF basada en Keccak para obtener una clave AES de 128 bits.
// Esto evita usar una clave fija hardcodeada y permite usar material secreto
// de mayor tamaño sin romper el tamaño de clave esperado por AES-CCM128.
std::array<unsigned char, AESCCM128::KEY_SIZE> AESCCM128::DeriveKey_(const Bytes& key_material) const {
  if (key_material.GetBytesSize() == 0) {
    throw std::runtime_error("ERROR: Empty AES-CCM key material.");
  }

  Bytes domain_separator("AES-CCM128-KEY");
  Bytes derived_key = Keccak::KDF(domain_separator + key_material, KEY_SIZE);
  std::vector<unsigned char> derived_key_vector = derived_key.GetBytes();

  std::array<unsigned char, KEY_SIZE> key{};
  for (int i = 0; i < KEY_SIZE; ++i) {
    key[i] = derived_key_vector[i];
  }

  return key;
}

std::pair<Bytes, Bytes> AESCCM128::Encrypt(const Bytes& message) {
  Bytes internal_key_material(secret_key_);
  return EncryptWithKey(message, internal_key_material);
}

// Cifra la semilla de Kyber con ASCON-AEAD128.
// El resultado incluye nonce + ciphertext + tag de autenticación.
// Este ciphertext será posteriormente insertado en la clave pública modificada.
std::pair<Bytes, Bytes> AESCCM128::EncryptWithKey(const Bytes& message, const Bytes& key_material) {
  Bytes nonce = GenerateNonce_();
  Bytes associated_data = BuildAssociatedData_();
  std::array<unsigned char, KEY_SIZE> key = DeriveKey_(key_material);

  std::vector<unsigned char> plaintext = message.GetBytes();
  std::vector<unsigned char> ad = associated_data.GetBytes();
  std::vector<unsigned char> nonce_vector = nonce.GetBytes();

  std::vector<unsigned char> ciphertext(plaintext.size());
  std::vector<unsigned char> tag(TAG_SIZE);

  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  if (ctx == nullptr) {
    throw std::runtime_error("ERROR: Unable to create AES-CCM context.");
  }

  int len = 0;

  if (EVP_EncryptInit_ex(ctx, EVP_aes_128_ccm(), nullptr, nullptr, nullptr) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM encrypt init failed.");
  }

  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_CCM_SET_IVLEN, NONCE_SIZE, nullptr) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM nonce size setup failed.");
  }

  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_CCM_SET_TAG, TAG_SIZE, nullptr) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM tag size setup failed.");
  }

  if (EVP_EncryptInit_ex(ctx, nullptr, nullptr, key.data(), nonce_vector.data()) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM key/nonce setup failed.");
  }

  if (EVP_EncryptUpdate(ctx, nullptr, &len, nullptr, plaintext.size()) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM plaintext length setup failed.");
  }

  if (!ad.empty()) {
    if (EVP_EncryptUpdate(ctx, nullptr, &len, ad.data(), ad.size()) != 1) {
      EVP_CIPHER_CTX_free(ctx);
      throw std::runtime_error("ERROR: AES-CCM associated data setup failed.");
    }
  }

  if (EVP_EncryptUpdate(ctx, ciphertext.data(), &len, plaintext.data(), plaintext.size()) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM encryption failed.");
  }

  if (EVP_EncryptFinal_ex(ctx, ciphertext.data() + len, &len) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM encrypt final failed.");
  }

  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_CCM_GET_TAG, TAG_SIZE, tag.data()) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM tag extraction failed.");
  }

  EVP_CIPHER_CTX_free(ctx);

  std::vector<unsigned char> output;
  output.insert(output.end(), nonce_vector.begin(), nonce_vector.end());
  output.insert(output.end(), ciphertext.begin(), ciphertext.end());
  output.insert(output.end(), tag.begin(), tag.end());

  Bytes cyphertext_bytes(output);

  return {cyphertext_bytes, message};
}

// Recupera la semilla cifrada usando el material secreto proporcionado.
// Si la autenticación falla, significa que el ciphertext o la clave no son válidos.
Bytes AESCCM128::Decrypt(const Bytes& cyphertext, const Bytes& sk) {
  std::vector<unsigned char> input = cyphertext.GetBytes();

  if (input.size() < NONCE_SIZE + TAG_SIZE) {
    throw std::runtime_error("ERROR: Invalid AES-CCM ciphertext size.");
  }

  Bytes key_material = sk;
  if (key_material.GetBytesSize() == 0) {
    key_material = Bytes(secret_key_);
  }

  std::array<unsigned char, KEY_SIZE> key = DeriveKey_(key_material);

  std::vector<unsigned char> nonce(
    input.begin(),
    input.begin() + NONCE_SIZE
  );

  std::vector<unsigned char> tag(
    input.end() - TAG_SIZE,
    input.end()
  );

  std::vector<unsigned char> encrypted_seed(
    input.begin() + NONCE_SIZE,
    input.end() - TAG_SIZE
  );

  Bytes associated_data = BuildAssociatedData_();
  std::vector<unsigned char> ad = associated_data.GetBytes();

  std::vector<unsigned char> plaintext(encrypted_seed.size());

  EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
  if (ctx == nullptr) {
    throw std::runtime_error("ERROR: Unable to create AES-CCM context.");
  }

  int len = 0;

  if (EVP_DecryptInit_ex(ctx, EVP_aes_128_ccm(), nullptr, nullptr, nullptr) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM decrypt init failed.");
  }

  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_CCM_SET_IVLEN, NONCE_SIZE, nullptr) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM nonce size setup failed.");
  }

  if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_CCM_SET_TAG, TAG_SIZE, tag.data()) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM tag setup failed.");
  }

  if (EVP_DecryptInit_ex(ctx, nullptr, nullptr, key.data(), nonce.data()) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM key/nonce setup failed.");
  }

  if (EVP_DecryptUpdate(ctx, nullptr, &len, nullptr, encrypted_seed.size()) != 1) {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("ERROR: AES-CCM ciphertext length setup failed.");
  }

  if (!ad.empty()) {
    if (EVP_DecryptUpdate(ctx, nullptr, &len, ad.data(), ad.size()) != 1) {
      EVP_CIPHER_CTX_free(ctx);
      throw std::runtime_error("ERROR: AES-CCM associated data setup failed.");
    }
  }

  int status = EVP_DecryptUpdate(
      ctx,
      plaintext.data(),
      &len,
      encrypted_seed.data(),
      encrypted_seed.size()
  );

  EVP_CIPHER_CTX_free(ctx);

  if (status != 1) {
    throw std::runtime_error("ERROR: AES-CCM authentication failed.");
  }

  plaintext.resize(len);
  return Bytes(plaintext);
}