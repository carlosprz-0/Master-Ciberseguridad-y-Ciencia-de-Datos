/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date July 18 2024
 * @version v0.1.0
 * @brief This file contains the tests for the Keccak class
 */

#include <gtest/gtest.h>

#include "../src/Components/Keccak.h"

class KeccakTest : public ::testing::Test {
 public:
  Bytes test_byte = Bytes("Hello World");
 protected:
  virtual void SetUp() {
    // Set up the test cases
  }
  
  virtual void TearDown() {
    // Clean up the test cases
  }
};

/**
 * @brief Test for the H() method.
 */
TEST_F(KeccakTest, HTest) {
  Bytes bytes = Keccak::H(test_byte, 32);
  EXPECT_EQ(bytes.FromBytesToHex(), "840d1ce81a4327840b54cb1d419907fd1f62359bad33656e058653d2e4172a43");
}

/**
 * @brief Test for the XOF() method.
 */
TEST_F(KeccakTest, XOFTest) {
  Bytes bytes = Keccak::XOF(test_byte, test_byte, test_byte, 32);
  EXPECT_EQ(bytes.FromBytesToHex(), "c3e6cbb65bb9d9a4da246b73af5d5a7277a6bc8eb18a95549c540e7d2dec4864");
}

/**
 * @brief Test for the PRF() method.
 */
TEST_F(KeccakTest, PRFTest) {
  Bytes bytes = Keccak::PRF(test_byte, 128, 32);
  EXPECT_EQ(bytes.FromBytesToHex(), "4b82342367348e596dec9ad90bb6959d6e03d967089a2e69785b794424918b08");
}

/**
 * @brief Test for the KDF() method.
 */
TEST_F(KeccakTest, KDFTest) {
  Bytes bytes = Keccak::KDF(test_byte, 32);
  EXPECT_EQ(bytes.FromBytesToHex(), "840d1ce81a4327840b54cb1d419907fd1f62359bad33656e058653d2e4172a43");
}

/**
 * @brief Test for the G() method.
 */
TEST_F(KeccakTest, GTest) {
  std::pair<Bytes, Bytes> bytes = Keccak::G(test_byte);
  EXPECT_EQ(bytes.first.FromBytesToHex(), "3d58a719c6866b0214f96b0a67b37e51a91e233ce0be126a08f35fdf4c043c61");
  EXPECT_EQ(bytes.second.FromBytesToHex(), "26f40139bfbc338d44eb2a03de9f7bb8eff0ac260b3629811e389a5fbee8a894");
}