/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date September 29 2024
 * @version v0.1.0
 * @brief This file contains the definition of the methods of the mceliece--460896 cypher class
 */


#include "./mceliece-460896.h"

/**
 * @brief Construct a new McEliece_460896::McEliece_460896 object
 */
McEliece_460896::McEliece_460896() {
  // First, we need to initialize the McEliece-460896 cypher
  kem = OQS_KEM_new(OQS_KEM_alg_classic_mceliece_460896);
  if (kem == nullptr) {
    throw std::runtime_error("ERROR: Unable to initialize the McEliece-460896 cypher.");
  }
  
  // We need to assign the length of the public and secret keys
  public_key_.resize(kem->length_public_key);
  secret_key_.resize(kem->length_secret_key);

  // Generate the keypair
  OQS_STATUS status = OQS_KEM_keypair(kem, public_key_.data(), secret_key_.data());
  if (status != OQS_SUCCESS) {
    OQS_KEM_free(kem);
    throw std::runtime_error("ERROR: Unable to generate the keypair for the McEliece-460896 cypher.");
  }
}

/**
 * @brief Encrypts a message using the McEliece-460896 cypher
 * @param pk The pk to be encrypted
 * @return Bytes The cyphertext
 */
std::pair<Bytes, Bytes> McEliece_460896::Encrypt(const Bytes& pk) {
  // We need to assign the length of the cyphertext
  std::vector<uint8_t> ciphertext(kem->length_ciphertext);   // Espacio para ctbd (ciphertext)
  std::vector<uint8_t> shared_secret(kem->length_shared_secret);  // Espacio para mbd (shared secret)

  std::vector<uint8_t> aux_pk = pk.GetBytes();
  
  // Encrypt the message
  OQS_STATUS status = OQS_KEM_classic_mceliece_460896_encaps(ciphertext.data(), shared_secret.data(), aux_pk.data());
  if (status != OQS_SUCCESS) {
    throw std::runtime_error("ERROR: Unable to encrypt the message using the McEliece-460896 cypher.");
  }
  Bytes cyphertext(ciphertext);
  Bytes shared_secret_bytes(shared_secret);
  return {cyphertext, shared_secret_bytes};
}


/**
 * @brief Decrypts a cyphertext using the McEliece-460896 cypher
 * @param cyphertext The cyphertext to be decrypted
 * @return Bytes The decrypted message
 */
Bytes McEliece_460896::Decrypt(const Bytes& cyphertext, const Bytes& sk) {
  if (sk.GetBytesSize() != 0) {
    secret_key_ = sk.GetBytes();
  }

  std::vector<uint8_t> aux_cyphertext = cyphertext.GetBytes();
  std::vector<uint8_t> shared_secret(kem->length_shared_secret);
  // Decrypt the message
  OQS_STATUS status = OQS_KEM_classic_mceliece_460896_decaps(shared_secret.data(), aux_cyphertext.data(), secret_key_.data());
  if (status != OQS_SUCCESS) {
    throw std::runtime_error("ERROR: Unable to decrypt the message using the McEliece-460896 cypher.");
  }

  Bytes shared_secret_bytes(shared_secret);
  return shared_secret_bytes;
}