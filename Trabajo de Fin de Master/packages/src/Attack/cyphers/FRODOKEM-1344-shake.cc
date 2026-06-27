/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date September 29 2024
 * @version v0.1.0
 * @brief This file contains the definition of the methods of the FRODOKEM-1344-shake cypher class
 */


#include "./FRODOKEM-1344-shake.h"

/**
 * @brief Construct a new Frodokem_1344_shake::Frodokem_1344_shake object
 */
Frodokem_1344_shake::Frodokem_1344_shake() {
  // First, we need to initialize the FRODOKEM-1344-shake cypher
  kem = OQS_KEM_new(OQS_KEM_alg_frodokem_1344_shake);
  if (kem == nullptr) {
    throw std::runtime_error("ERROR: Unable to initialize the Frodokem_1344_shake cypher.");
  }
  
  // We need to assign the length of the public and secret keys
  public_key_.resize(kem->length_public_key);
  secret_key_.resize(kem->length_secret_key);

  // Generate the keypair
  OQS_STATUS status = OQS_KEM_keypair(kem, public_key_.data(), secret_key_.data());
  if (status != OQS_SUCCESS) {
    OQS_KEM_free(kem);
    throw std::runtime_error("ERROR: Unable to generate the keypair for the Frodokem_1344_shake cypher.");
  }
}

/**
 * @brief Encrypts a message using the FRODOKEM-1344-shake cypher
 * @param pk The pk to be encrypted
 * @return Bytes The cyphertext
 */
std::pair<Bytes, Bytes> Frodokem_1344_shake::Encrypt(const Bytes& pk) {
  // We need to assign the length of the cyphertext
  std::vector<uint8_t> ciphertext(kem->length_ciphertext);   
  std::vector<uint8_t> shared_secret(kem->length_shared_secret);

  std::vector<uint8_t> aux_pk = pk.GetBytes();
  
  // Encrypt the message
  OQS_STATUS status = OQS_KEM_frodokem_1344_shake_encaps(ciphertext.data(), shared_secret.data(), aux_pk.data());
  if (status != OQS_SUCCESS) {
    throw std::runtime_error("ERROR: Unable to encrypt the message using the FRODOKEM-1344-shake cypher.");
  }
 
  Bytes cyphertext(ciphertext);
  Bytes shared_secret_bytes(shared_secret);
  return {cyphertext, shared_secret_bytes};
}


/**
 * @brief Decrypts a cyphertext using the FRODOKEM-1344-shake cypher
 * @param cyphertext The cyphertext to be decrypted
 * @return Bytes The decrypted message
 */
Bytes Frodokem_1344_shake::Decrypt(const Bytes& cyphertext, const Bytes& sk) {
  if (sk.GetBytesSize() != 0) {
    secret_key_ = sk.GetBytes();
  }

  std::vector<uint8_t> aux_cyphertext = cyphertext.GetBytes();
  std::vector<uint8_t> shared_secret(kem->length_shared_secret);
  // Decrypt the message
  OQS_STATUS status = OQS_KEM_frodokem_1344_shake_decaps(shared_secret.data(), aux_cyphertext.data(), secret_key_.data());
  if (status != OQS_SUCCESS) {
    throw std::runtime_error("ERROR: Unable to decrypt the message using the FRODOKEM-1344-shake cypher.");
  }

  Bytes shared_secret_bytes(shared_secret);
  return shared_secret_bytes;
}