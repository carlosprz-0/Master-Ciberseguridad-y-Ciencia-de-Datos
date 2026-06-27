/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date July 18 2024
 * @version v0.1.0
 * @brief This file contains the tests for the Polynomial class
 
 */

#include <gtest/gtest.h>
#include "../src/DataStructures/Polynomial.h"

class PolynomialTest : public ::testing::Test {
 public:
  Polynomial<int> poly1_ = Polynomial<int>(3);
  Polynomial<int> poly2_ = Polynomial<int>(3);
  Polynomial<int> poly3_ = Polynomial<int>(3);
 protected:
  virtual void SetUp() {
    // Set up the test cases with polynomials of size 3
    // Initialize poly1_ to [1, 2, 3]
    poly1_[0] = 1;
    poly1_[1] = 2;
    poly1_[2] = 3;
    // Initialize poly2_ to [4, 5, 6]
    poly2_[0] = 4;
    poly2_[1] = 5;
    poly2_[2] = 6;
    // Initialize poly3_ to [0, 0, 0] (default initialization)
  }
  
  virtual void TearDown() {
    // Clean up the test cases
  }
};

/**
 * @brief Test for the GetSize() method.
 */
TEST_F(PolynomialTest, GetSizeTest) {
  EXPECT_EQ(poly1_.GetSize(), 3);
  EXPECT_EQ(poly2_.GetSize(), 3);
  EXPECT_EQ(poly3_.GetSize(), 3);
}

/**
 * @brief Test for the operator[] method (const version).
 */
TEST_F(PolynomialTest, ConstSubscriptOperatorTest) {
  const Polynomial<int> poly1_const = poly1_;
  EXPECT_EQ(poly1_const[0], 1);
  EXPECT_EQ(poly1_const[1], 2);
  EXPECT_EQ(poly1_const[2], 3);
}

/**
 * @brief Test for the operator[] method.
 */
TEST_F(PolynomialTest, SubscriptOperatorTest) {
  EXPECT_EQ(poly1_[0], 1);
  EXPECT_EQ(poly1_[1], 2);
  EXPECT_EQ(poly1_[2], 3);
  poly1_[0] = 10;
  EXPECT_EQ(poly1_[0], 10);
}

/**
 * @brief Test for the operator+ method.
 */
TEST_F(PolynomialTest, AdditionOperatorTest) {
  Polynomial<int> result = poly1_ + poly2_;
  EXPECT_EQ(result[0], 5);
  EXPECT_EQ(result[1], 7);
  EXPECT_EQ(result[2], 9);
}

/**
 * @brief Test for the operator<< method.
 */
TEST_F(PolynomialTest, OutputStreamOperatorTest) {
  std::stringstream ss;
  ss << poly1_;
  EXPECT_EQ(ss.str(), "[1, 2, 3]");
}

/**
 * @brief Test for assertion failure in operator+ due to different sizes.
 */
TEST_F(PolynomialTest, AdditionOperatorDifferentSizeTest) {
  Polynomial<int> poly4(4); // A polynomial of different size
  EXPECT_DEATH(poly1_ + poly4, ".*");
}

/**
 * @brief Test for assertion failure in operator[] due to out-of-bounds index.
 */
TEST_F(PolynomialTest, SubscriptOperatorOutOfBoundsTest) {
  EXPECT_DEATH(poly1_[3], ".*");
}

/**
 * @brief Test for assertion failure in operator[] due to out-of-bounds index (const version).
 */
TEST_F(PolynomialTest, ConstSubscriptOperatorOutOfBoundsTest) {
  const Polynomial<int> poly1_const = poly1_;
  EXPECT_DEATH(poly1_const[3], ".*");
}

/**
 * @brief Test for addition of polynomials
 */
TEST_F(PolynomialTest, AdditionOperatorGlobalTest) {
  Polynomial<int> result = poly1_ + poly2_;
  EXPECT_EQ(result[0], 5);
  EXPECT_EQ(result[1], 7);
  EXPECT_EQ(result[2], 9);
}
