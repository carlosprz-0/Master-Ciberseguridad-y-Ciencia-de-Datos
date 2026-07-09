/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date August 20 2024
 * @version v0.1.0
 * @brief This file contains the definition of the methods of the KletoKyber class 
 */

#include "./KleptoKyber.h"

/**
 * @brief Construct a new Klepto Kyber:: Klepto Kyber object
 * 
 * @param option The option of the Kyber cryptosystem
 * @param attacker_pk The attacker public key
 * @param attacker_sk The attacker secret key
 * @param seed The seed to generate the rho and sigma values
 */
KleptoKyber::KleptoKyber(int option, Bytes attacker_pk, Bytes attacker_sk, const std::vector<int>& seed, int cypher) : Kyber(option, seed, cypher) {
  attacker_pk_ = attacker_pk;
  attacker_sk_ = attacker_sk;
  cypher_option_ = cypher;
}

/**
 * @brief This function generates a seed of a given size
 * @return std::pair<Bytes, Bytes>, where pk and sk
 */
std::pair<Bytes, Bytes> KleptoKyber::RunBackdoor()  {
  
  /*
  // We use the public key of the attacker to encrypt the shared secret
  std::pair<Bytes, Bytes> pair_ct_sharedm = cypher_box_->Encrypt(attacker_pk_);  

  // std::cout << "Ciphertext: " << pair_ct_sharedm.first.FromBytesToHex() << std::endl;
  Bytes seed_a = GenerateSeed_(seed_size_);
  Bytes seed_b = pair_ct_sharedm.second;
  */
  Bytes seed_a = GenerateSeed_(seed_size_);
  Bytes seed_b;
  std::pair<Bytes, Bytes> pair_ct_sharedm;

  if (cypher_option_ == ASCON_AEAD128) {
  seed_b = GenerateSeed_(seed_size_);

  AsconAEAD128* ascon_box = dynamic_cast<AsconAEAD128*>(cypher_box_.get());
  if (ascon_box == nullptr) {
    throw std::runtime_error("ERROR: Invalid ASCON cypher box.");
  }

  pair_ct_sharedm = ascon_box->EncryptWithKey(seed_b, attacker_pk_);

} else if (cypher_option_ == AES_CCM_128) {
  seed_b = GenerateSeed_(seed_size_);

  AESCCM128* aes_box = dynamic_cast<AESCCM128*>(cypher_box_.get());
  if (aes_box == nullptr) {
    throw std::runtime_error("ERROR: Invalid AES-CCM cypher box.");
  }

  pair_ct_sharedm = aes_box->EncryptWithKey(seed_b, attacker_pk_);

} else {
  pair_ct_sharedm = cypher_box_->Encrypt(attacker_pk_);
  seed_b = pair_ct_sharedm.second;
}

  Matrix<Polynomial<int>> a = ntt_->GenerateMatrix_(k_, seed_a, true);
  int N = 0;
  std::pair<Matrix<Polynomial<int>>, int> pair_s_counter = sampling_unit_->GenerateDistribuitionMatrix(seed_b, n1_, N);
    // std::cout << "s: " << pair_s_counter.first << std::endl;
  Matrix<Polynomial<int>> s_ntt = applyNTTMatrix_(pair_s_counter.first, k_);

  std::pair<Matrix<Polynomial<int>>, int> pair_e_counter = sampling_unit_->GenerateDistribuitionMatrix(seed_b, n1_, pair_s_counter.second);
  Matrix<Polynomial<int>> e_ntt = applyNTTMatrix_(pair_e_counter.first, k_);
  // Start of the backdoor

  // 1. t = INTT(â ◦ NTT(s)) + e
  Matrix<Polynomial<int>> t = applyNTTMatrix_(ntt_->multMatrixViaNTT(a, s_ntt), k_, false) + e_ntt;
  // ctbd = (bt0 , bt1 , . . . , btb ); c = ⌈b/(k · n)⌉
  int b = pair_ct_sharedm.first.GetBytesSize() * BYTE_SIZE;
  ct_size_ = b;
  int c = b / (k_ * n_) + 1;
  
  // Initialize the polynomial p with c length
  Polynomial<int> p(k_ * n_);
  int size_p_without_zeros = b / c;
  // 2. Pack the bits of the ciphertext into the polynomial p
  p = PackBitsIntoPolynomial_(pair_ct_sharedm.first, t, c);


  // 3. Compute the compensation polynomial
  Polynomial<int> t_polynomial = t.GetPolynomialFromMatrix(n_);
  Polynomial<int> h = ComputeCompensation_(p, t_polynomial, c);
  // 4. t' = t + h
  Polynomial<int> t_prime = t_polynomial + h;
  // 5. Checks
  bool is_working_well = IsWorkingWell_(p, t_polynomial, h, c);
  if (!is_working_well) {
    std::cerr << "Error: The backdoor is not working well" << std::endl;
    exit(1);
  }
  
  // 6. Convert t' into a matrix
  Matrix<Polynomial<int>> t_prime_matrix(k_, 1);
  int current_row = 0;
  for (int i = 0; i < t_prime.GetSize(); i += n_) {
    t_prime_matrix(current_row, 0) = ntt_->NTT_Kyber(t_prime.GetSubPolynomial(i, i + n_), true);
    current_row++;
  }
  return std::pair<Bytes, Bytes>{seed_a + encdec_unit_->EncodeMatrixToBytes(t_prime_matrix, 12), encdec_unit_->EncodeMatrixToBytes(s_ntt, 12)};
}


/**
 * @brief This function recovers the secret key of the Kyber cryptosystem
 * 
 * @param pk The public key of the attacker
 * @return Bytes
 */
Bytes KleptoKyber::recoverSecretKey(const Bytes& pk) const {
  Bytes seed_a = pk.GetNBytes(0, 32);
  Bytes t_prime_bytes = pk.GetNBytes(32, pk.GetBytesSize() - 32);
  // 1. Recover t'
  Matrix<Polynomial<int>> t_prime = encdec_unit_->DecodeBytesToMatrix(t_prime_bytes, k_, 1, 12);
  // 2. Apply the INTT to t'
  t_prime = applyNTTMatrix_(t_prime, k_, false); 

  // Number of cyphertext bits
  int b = ct_size_;
  // Number of bits per coefficient
  int c = b / (k_ * n_) + 1;
  Polynomial<int> t_prime_polynomial = t_prime.GetPolynomialFromMatrix(n_);
  #ifdef DEBUG
  std::cout << "c: " << c << std::endl;
  #endif
  // 3. Reconstruct polynomial p from t_prime
  int size_of_p_without_zeros = b / c + 1;
  Polynomial<int> p(k_ * n_);
  for (int i = 0; i < size_of_p_without_zeros; ++i) {
    p[i] = t_prime_polynomial[i] % (2 << (c - 1));
  }

  std::string recovered_ct_str = "";

  for (int i = 0; i < (size_of_p_without_zeros - 1); ++i) {
    std::string bits = byteToReversedBits(p[i], c);
    recovered_ct_str += bits;
  }
  Bytes recovered_ct = Bytes::FromBitsToBytes(recovered_ct_str);
  Bytes m_prime = cypher_box_->Decrypt(recovered_ct, attacker_sk_);
  std::pair<Matrix<Polynomial<int>>, int> result_sk_n = sampling_unit_->GenerateDistribuitionMatrix(m_prime, n1_, 0);
    // std::cout << "SK N: " << result_sk_n.first << std::endl;

  Matrix<Polynomial<int>> sk_n_ntt = applyNTTMatrix_(result_sk_n.first, k_);
  return encdec_unit_->EncodeMatrixToBytes(sk_n_ntt, 12);
}


/**
 * @brief This function generates a seed of a given size
 * 
 * @param ct Ciphertext
 * @param t t matrix
 * @param c Number of bits per coefficient
 * @return Polynomial<int> 
 */
Polynomial<int> KleptoKyber::PackBitsIntoPolynomial_(const Bytes& ct, const Matrix<Polynomial<int>>& t, int c) const {
  int b = ct.GetBytesSize() * BYTE_SIZE;
  int size_p_without_zeros = b / c;
  Polynomial<int> p(k_ * n_);
  for (int i = 0; i < size_p_without_zeros; ++i) {
    p[i] = 0;
    for (int j = i * c; j < (i + 1) * c; ++j) {
      // Extract the bit from the corresponding position of the ciphertext
      int bit_j = ct.GetBit(j);
      // Pack this bit into the coefficient p[i] using bit shifting.
      p[i] += bit_j * (1 << (j - i * c));
    }
  }
  return p;
}

/**
 * @brief this function computes the compensation polynomial
 * 
 * @param p polynomial p
 * @param t_polynomial polynomial t
 * @param c number of bits per coefficient
 * @return Polynomial<int> 
 */
Polynomial<int> KleptoKyber::ComputeCompensation_(const Polynomial<int>& p, const Polynomial<int>& t_polynomial, int c) const {
  Polynomial<int> h(k_ * n_);
  for (int i = 0; i < p.GetSize(); ++i) {
    h[i] = (p[i] - t_polynomial[i])  % (2 << (c - 1));
  }
  return h;
}

/**
 * @brief This function checks if the backdoor is working well
 * 
 * @param p The polynomial p
 * @param t_polynomial The polynomial t
 * @param h The compensation polynomial h
 * @param c The number of bits per coefficient
 * @return true 
 * @return false 
 */
bool KleptoKyber::IsWorkingWell_(const Polynomial<int>& p, const Polynomial<int>& t_polynomial, const Polynomial<int>& h, int c) const {
  for (int i = 0; i < p.GetSize(); ++i) {
    if ((p[i] - t_polynomial[i]) % (2 << (c - 1)) != h[i]) {
      return false;
    }
  }
  return true;
}

/**
 * @brief This function converts a byte into a string of reversed bits
 * 
 * @param byte The byte to convert
 * @param numBits The number of bits to convert
 * @return std::string 
 */
std::string KleptoKyber::byteToReversedBits(unsigned char byte, int numBits) const {
  std::string bits;
  for (int i = 0; i < numBits; ++i) {
    bits += (byte & (1 << i)) ? '1' : '0';
  }
  return bits;
}