/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date July 18 2024
 * @version v0.1.0
 * @brief This file contains the tests for the SamplingUnit class
 
 */

#include <gtest/gtest.h>

#include "../src/Attack/KleptoKyber.h"

class AttackTest : public ::testing::Test {
 public:

  Kyber kyber_512_mceliece_348864 = Kyber(512, {}, MCELIECE_348864);
  Kyber kyber_512_mceliece_460896 = Kyber(512, {}, MCELIECE_460896);
  Kyber kyber_512_frodokem_1344_shake = Kyber(512, {}, FRODOKEM_1344_SHAKE);
  Kyber kyber_512_frodokem_640_shake = Kyber(512, {}, FRODOKEM_640_SHAKE);
  Kyber kyber_512_kyber_kem_512 = Kyber(512, {}, KYBER_KEM_512_OQS);

  Kyber kyber_768_mceliece_348864 = Kyber(768, {}, MCELIECE_348864);
  Kyber kyber_768_mceliece_460896 = Kyber(768, {}, MCELIECE_460896);
  Kyber kyber_768_frodokem_1344_shake = Kyber(768, {}, FRODOKEM_1344_SHAKE);
  Kyber kyber_768_frodokem_640_shake = Kyber(768, {}, FRODOKEM_640_SHAKE);
  Kyber kyber_768_kyber_kem_512 = Kyber(768, {}, KYBER_KEM_512_OQS);

  Kyber kyber_1024_mceliece_348864 = Kyber(1024, {}, MCELIECE_348864);
  Kyber kyber_1024_mceliece_460896 = Kyber(1024, {}, MCELIECE_460896);
  Kyber kyber_1024_frodokem_1344_shake = Kyber(1024, {}, FRODOKEM_1344_SHAKE);
  Kyber kyber_1024_frodokem_640_shake = Kyber(1024, {}, FRODOKEM_640_SHAKE);

  Kyber kyber_512_ascon = Kyber(512, {}, ASCON_AEAD128);
  Kyber kyber_768_ascon = Kyber(768, {}, ASCON_AEAD128);
  Kyber kyber_1024_ascon = Kyber(1024, {}, ASCON_AEAD128);

  Kyber kyber_512_aes_ccm128 = Kyber(512, {}, AES_CCM_128);
  Kyber kyber_768_aes_ccm128 = Kyber(768, {}, AES_CCM_128);
  Kyber kyber_1024_aes_ccm128 = Kyber(1024, {}, AES_CCM_128);

  std::pair<Bytes, Bytes> keypair_512_mceliece_348864 = kyber_512_mceliece_348864.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_512_mceliece_460896 = kyber_512_mceliece_460896.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_512_frodokem_1344_shake = kyber_512_frodokem_1344_shake.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_512_frodokem_640_shake = kyber_512_frodokem_640_shake.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_512_kyber_kem_512 = kyber_512_kyber_kem_512.KEMKeyGen();

  std::pair<Bytes, Bytes> keypair_768_mceliece_348864 = kyber_768_mceliece_348864.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_768_mceliece_460896 = kyber_768_mceliece_460896.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_768_frodokem_1344_shake = kyber_768_frodokem_1344_shake.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_768_frodokem_640_shake = kyber_768_frodokem_640_shake.KEMKeyGen();

  std::pair<Bytes, Bytes> keypair_1024_mceliece_348864 = kyber_1024_mceliece_348864.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_1024_mceliece_460896 = kyber_1024_mceliece_460896.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_1024_frodokem_1344_shake = kyber_1024_frodokem_1344_shake.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_1024_frodokem_640_shake = kyber_1024_frodokem_640_shake.KEMKeyGen();

  std::pair<Bytes, Bytes> keypair_512_ascon = kyber_512_ascon.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_768_ascon = kyber_768_ascon.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_1024_ascon = kyber_1024_ascon.KEMKeyGen();

  std::pair<Bytes, Bytes> keypair_512_aes_ccm128 = kyber_512_aes_ccm128.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_768_aes_ccm128 = kyber_768_aes_ccm128.KEMKeyGen();
  std::pair<Bytes, Bytes> keypair_1024_aes_ccm128 = kyber_1024_aes_ccm128.KEMKeyGen();

 protected:
  virtual void SetUp() {}
  virtual void TearDown() {}
};



TEST_F(AttackTest, attack_512_mceliece_348864) {
  KleptoKyber klepto_kyber_512_mceliece_348864 = KleptoKyber(512, keypair_512_mceliece_348864.first, keypair_512_mceliece_348864.second, {}, MCELIECE_348864);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_512_mceliece_348864.RunBackdoor();
  EXPECT_EQ(klepto_kyber_512_mceliece_348864.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_512_mceliece_460896) {
  KleptoKyber klepto_kyber_512_mceliece_460896 = KleptoKyber(512, keypair_512_mceliece_460896.first, keypair_512_mceliece_460896.second, {}, MCELIECE_460896);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_512_mceliece_460896.RunBackdoor();
  EXPECT_EQ(klepto_kyber_512_mceliece_460896.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_512_frodokem_1344_shake) {
  KleptoKyber klepto_kyber_512_frodokem_1344_shake = KleptoKyber(512, keypair_512_frodokem_1344_shake.first, keypair_512_frodokem_1344_shake.second, {}, FRODOKEM_1344_SHAKE);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_512_frodokem_1344_shake.RunBackdoor();
  EXPECT_NE(klepto_kyber_512_frodokem_1344_shake.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_512_frodokem_640_shake) {
  KleptoKyber klepto_kyber_512_frodokem_640_shake = KleptoKyber(512, keypair_512_frodokem_640_shake.first, keypair_512_frodokem_640_shake.second, {}, FRODOKEM_640_SHAKE);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_512_frodokem_640_shake.RunBackdoor();
  EXPECT_NE(klepto_kyber_512_frodokem_640_shake.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_512_kyber_kem_512) {
  KleptoKyber klepto_kyber_512_kyber_kem_512 = KleptoKyber(512, keypair_512_kyber_kem_512.first, keypair_512_kyber_kem_512.second, {}, KYBER_KEM_512_OQS);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_512_kyber_kem_512.RunBackdoor();
  EXPECT_NE(klepto_kyber_512_kyber_kem_512.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_768_mceliece_348864) {
  KleptoKyber klepto_kyber_768_mceliece_348864 = KleptoKyber(768, keypair_768_mceliece_348864.first, keypair_768_mceliece_348864.second, {}, MCELIECE_348864);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_768_mceliece_348864.RunBackdoor();
  EXPECT_EQ(klepto_kyber_768_mceliece_348864.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_768_mceliece_460896) {
  KleptoKyber klepto_kyber_768_mceliece_460896 = KleptoKyber(768, keypair_768_mceliece_460896.first, keypair_768_mceliece_460896.second, {}, MCELIECE_460896);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_768_mceliece_460896.RunBackdoor();
  EXPECT_EQ(klepto_kyber_768_mceliece_460896.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_768_frodokem_1344_shake) {
  KleptoKyber klepto_kyber_768_frodokem_1344_shake = KleptoKyber(768, keypair_768_frodokem_1344_shake.first, keypair_768_frodokem_1344_shake.second, {}, FRODOKEM_1344_SHAKE);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_768_frodokem_1344_shake.RunBackdoor();
  EXPECT_NE(klepto_kyber_768_frodokem_1344_shake.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_768_frodokem_640_shake) {
  KleptoKyber klepto_kyber_768_frodokem_640_shake = KleptoKyber(768, keypair_768_frodokem_640_shake.first, keypair_768_frodokem_640_shake.second, {}, FRODOKEM_640_SHAKE);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_768_frodokem_640_shake.RunBackdoor();
  EXPECT_NE(klepto_kyber_768_frodokem_640_shake.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_1024_mceliece_348864) {
  KleptoKyber klepto_kyber_1024_mceliece_348864 = KleptoKyber(1024, keypair_1024_mceliece_348864.first, keypair_1024_mceliece_348864.second, {}, MCELIECE_348864);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_1024_mceliece_348864.RunBackdoor();
  EXPECT_EQ(klepto_kyber_1024_mceliece_348864.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_1024_mceliece_460896) {
  KleptoKyber klepto_kyber_1024_mceliece_460896 = KleptoKyber(1024, keypair_1024_mceliece_460896.first, keypair_1024_mceliece_460896.second, {}, MCELIECE_460896);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_1024_mceliece_460896.RunBackdoor();
  EXPECT_EQ(klepto_kyber_1024_mceliece_460896.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_1024_frodokem_1344_shake) {
  KleptoKyber klepto_kyber_1024_frodokem_1344_shake = KleptoKyber(1024, keypair_1024_frodokem_1344_shake.first, keypair_1024_frodokem_1344_shake.second, {}, FRODOKEM_1344_SHAKE);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_1024_frodokem_1344_shake.RunBackdoor();
  EXPECT_NE(klepto_kyber_1024_frodokem_1344_shake.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

TEST_F(AttackTest, attack_1024_frodokem_640_shake) {
  KleptoKyber klepto_kyber_1024_frodokem_640_shake = KleptoKyber(1024, keypair_1024_frodokem_640_shake.first, keypair_1024_frodokem_640_shake.second, {}, FRODOKEM_640_SHAKE);
  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_1024_frodokem_640_shake.RunBackdoor();
  EXPECT_NE(klepto_kyber_1024_frodokem_640_shake.recoverSecretKey(key_pair_backdoored.first), key_pair_backdoored.second);
}

  
// Tests añadidos para comprobar que la puerta trasera funciona correctamente
// cuando la semilla de Kyber se protege mediante ASCON-AEAD128.
TEST_F(AttackTest, attack_512_ascon_aead128) {
  Kyber kyber_512_ascon = Kyber(512, {}, ASCON_AEAD128);
  std::pair<Bytes, Bytes> keypair_512_ascon = kyber_512_ascon.KEMKeyGen();

  KleptoKyber klepto_kyber_512_ascon =
      KleptoKyber(512, keypair_512_ascon.first, keypair_512_ascon.second, {}, ASCON_AEAD128);

  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_512_ascon.RunBackdoor();

  EXPECT_EQ(
      klepto_kyber_512_ascon.recoverSecretKey(key_pair_backdoored.first),
      key_pair_backdoored.second
  );
}

// Tests añadidos para validar la alternativa AES-CCM128.
// Se comprueba que la clave secreta recuperada coincide con la clave secreta generada.
TEST_F(AttackTest, attack_512_aes_ccm128) {
  KleptoKyber klepto_kyber_512_aes_ccm128 =
      KleptoKyber(512, keypair_512_aes_ccm128.first, keypair_512_aes_ccm128.second, {}, AES_CCM_128);

  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_512_aes_ccm128.RunBackdoor();

  EXPECT_EQ(
      klepto_kyber_512_aes_ccm128.recoverSecretKey(key_pair_backdoored.first),
      key_pair_backdoored.second
  );
}

TEST_F(AttackTest, attack_768_aes_ccm128) {
  KleptoKyber klepto_kyber_768_aes_ccm128 =
      KleptoKyber(768, keypair_768_aes_ccm128.first, keypair_768_aes_ccm128.second, {}, AES_CCM_128);

  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_768_aes_ccm128.RunBackdoor();

  EXPECT_EQ(
      klepto_kyber_768_aes_ccm128.recoverSecretKey(key_pair_backdoored.first),
      key_pair_backdoored.second
  );
}

TEST_F(AttackTest, attack_1024_aes_ccm128) {
  KleptoKyber klepto_kyber_1024_aes_ccm128 =
      KleptoKyber(1024, keypair_1024_aes_ccm128.first, keypair_1024_aes_ccm128.second, {}, AES_CCM_128);

  std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber_1024_aes_ccm128.RunBackdoor();

  EXPECT_EQ(
      klepto_kyber_1024_aes_ccm128.recoverSecretKey(key_pair_backdoored.first),
      key_pair_backdoored.second
  );
}


