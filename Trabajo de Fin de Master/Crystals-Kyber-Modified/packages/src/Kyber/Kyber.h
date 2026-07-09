/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date August 20 2024
 * @version v0.1.0
 * @brief This file contains the header declaration of the class Kyber which contains the implementation of the Kyber cryptosystem
 */

#pragma once

#include <stdarg.h>


#include "../Components/NTT.h"
#include "../Components/Keccak.h"
#include "../Components/EncDecUnit.h"
#include "../Components/SamplingUnit.h"
#include "../Components/CompressorUnit.h"

#include "../DataStructures/Bytes.h"
#include "../DataStructures/Matrix.h"

#include "../Attack/cyphers/mceliece-348864.h"
#include "../Attack/cyphers/mceliece-460896.h"
#include "../Attack/cyphers/FRODOKEM-640-shake.h"
#include "../Attack/cyphers/FRODOKEM-1344-shake.h"
#include "../Attack/cyphers/KYBER-KEM-512.h"
#include "../Attack/cyphers/ASCON-AEAD128.h"
#include "../Attack/cyphers/AES-CCM128.h"


#define KYBER_CBOX 0
#define MCELIECE_348864 1
#define MCELIECE_460896 2
#define FRODOKEM_1344_SHAKE 3
#define FRODOKEM_640_SHAKE 4
#define KYBER_KEM_512_OQS 5
#define ASCON_AEAD128 6
#define AES_CCM_128 7

class Kyber {
 public:
  Kyber(int option, const std::vector<int>& seed = {}, int cypher_box_option = KYBER_CBOX);
  
  std::pair<Bytes, Bytes> KeyGen() const;
  Bytes Encryption(const Bytes& pk, const Bytes& message, const Bytes& seed) const;  
  Bytes Decryption(const Bytes& sk, const Bytes& ciphertext) const;
  
  std::pair<Bytes, Bytes> KEMKeyGen() const;
  std::pair<Bytes, Bytes> KEMEncapsulation(const Bytes& pk) const;
  Bytes KEMDecapsulation(const Bytes& sk, const Bytes& ciphertext) const;

 protected:
  Bytes GenerateSeed_(int seed_size) const;
  std::pair<Bytes, Bytes> GenerateRhoSigma_(const Bytes& seed) const;
  Matrix<Polynomial<int>> applyNTTMatrix_(const Matrix<Polynomial<int>>& matrix, int k, bool is_ntt = true) const;
  void InitializeConstants(int option);

  int n_;
  int q_;
  int k_;
  int n1_;
  int n2_;
  int du_;
  int dv_;
  int seed_size_;
  int ct_size_;
  int pk_size_;
  int sk_size_;

  
  std::unique_ptr<NTT> ntt_ = nullptr;
  std::unique_ptr<EncDecUnit> encdec_unit_ = nullptr;
  std::unique_ptr<SamplingUnit> sampling_unit_ = nullptr;
  std::unique_ptr<CompressorUnit> compressor_unit_ = nullptr;

  std::unique_ptr<Bytes> pk_ = nullptr;
  std::unique_ptr<Bytes> sk_ = nullptr;
  
  std::vector<int> seed_ = {};
  std::unique_ptr<Cypher> cypher_box_ = nullptr;
};