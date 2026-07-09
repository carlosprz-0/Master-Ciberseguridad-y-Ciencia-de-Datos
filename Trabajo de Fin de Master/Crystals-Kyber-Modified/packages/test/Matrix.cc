/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date July 18 2024
 * @version v0.1.0
 * @brief This file contains the tests for the Matrix class
 
 */

#include <gtest/gtest.h>
#include "../src/DataStructures/Matrix.h"
#include "../src/DataStructures/Polynomial.h"

class MatrixTest : public ::testing::Test {
 public:
  Matrix<int> mat1_ = Matrix<int>(2, 2);
  Matrix<int> mat2_ = Matrix<int>(2, 2);
  Matrix<int> mat3_ = Matrix<int>(2, 2);
  Matrix<Polynomial<int>> mat4_ = Matrix<Polynomial<int>>(2, 2, 3);
  Matrix<Polynomial<int>> mat5_ = Matrix<Polynomial<int>>(2, 2, 3);
 protected:
  virtual void SetUp() {
    // Set up the test cases with matrices of size 2x2
    // Initialize mat1_ to [[1, 2], [3, 4]]
    mat1_(0, 0) = 1;
    mat1_(0, 1) = 2;
    mat1_(1, 0) = 3;
    mat1_(1, 1) = 4;
    // Initialize mat2_ to [[5, 6], [7, 8]]
    mat2_(0, 0) = 5;
    mat2_(0, 1) = 6;
    mat2_(1, 0) = 7;
    mat2_(1, 1) = 8;
    // Initialize mat3_ to [[0, 0], [0, 0]] (default initialization)

    // Initialize mat4_ to ([x^2 + 2x + 3, 2x^2 + 3x + 4], [3x^2 + 4x + 5, 4x^2 + 5x + 6])
    Polynomial<int> p1 = Polynomial<int>(3);
    p1[0] = 3;
    p1[1] = 2;
    p1[2] = 1;
    Polynomial<int> p2 = Polynomial<int>(3);
    p2[0] = 4;
    p2[1] = 3;
    p2[2] = 2;
    mat4_(0, 0) = p1;
    mat4_(0, 1) = p2;
    mat5_(0, 0) = p1;
  }
  
  virtual void TearDown() {
    // Clean up the test cases
  }
};

/**
 * @brief Test for the GetColumnsSize() and GetRowsSize() methods.
 */
TEST_F(MatrixTest, GetSizeTest) {
  EXPECT_EQ(mat1_.GetRowsSize(), 2);
  EXPECT_EQ(mat1_.GetColumnsSize(), 2);
  EXPECT_EQ(mat2_.GetRowsSize(), 2);
  EXPECT_EQ(mat2_.GetColumnsSize(), 2);
  EXPECT_EQ(mat3_.GetRowsSize(), 2);
  EXPECT_EQ(mat3_.GetColumnsSize(), 2);
}

/**
 * @brief Test for the operator() method (const version).
 */
TEST_F(MatrixTest, ConstSubscriptOperatorTest) {
  const Matrix<int> mat1_const = mat1_;
  EXPECT_EQ(mat1_const(0, 0), 1);
  EXPECT_EQ(mat1_const(0, 1), 2);
  EXPECT_EQ(mat1_const(1, 0), 3);
  EXPECT_EQ(mat1_const(1, 1), 4);
}

/**
 * @brief Test for the operator() method.
 */
TEST_F(MatrixTest, SubscriptOperatorTest) {
  EXPECT_EQ(mat1_(0, 0), 1);
  EXPECT_EQ(mat1_(0, 1), 2);
  EXPECT_EQ(mat1_(1, 0), 3);
  EXPECT_EQ(mat1_(1, 1), 4);
  mat1_(0, 0) = 10;
  EXPECT_EQ(mat1_(0, 0), 10);
}

/**
 * @brief Test for the operator+ method.
 */
TEST_F(MatrixTest, AdditionOperatorTest) {
  Matrix<int> result = mat1_ + mat2_;
  EXPECT_EQ(result(0, 0), 6);
  EXPECT_EQ(result(0, 1), 8);
  EXPECT_EQ(result(1, 0), 10);
  EXPECT_EQ(result(1, 1), 12);
}

/**
 * @brief Test for the operator<< method.
 */
TEST_F(MatrixTest, OutputStreamOperatorTest) {
  std::stringstream ss;
  ss << mat1_;
  EXPECT_EQ(ss.str(), "(1, 2)\n(3, 4)\n");
}

/**
 * @brief Test for assertion failure in operator+ due to different sizes.
 */
TEST_F(MatrixTest, AdditionOperatorDifferentSizeTest) {
  Matrix<int> mat4(3, 3); // A matrix of different size
  EXPECT_DEATH(mat1_ + mat4, ".*");
}

/**
 * @brief Test for assertion failure in operator() due to out-of-bounds index.
 */
TEST_F(MatrixTest, SubscriptOperatorOutOfBoundsTest) {
  EXPECT_DEATH(mat1_(2, 0), ".*");
  EXPECT_DEATH(mat1_(0, 2), ".*");
}

/**
 * @brief Test for assertion failure in operator() due to out-of-bounds index (const version).
 */
TEST_F(MatrixTest, ConstSubscriptOperatorOutOfBoundsTest) {
  const Matrix<int> mat1_const = mat1_;
  EXPECT_DEATH(mat1_const(2, 0), ".*");
  EXPECT_DEATH(mat1_const(0, 2), ".*");
}

/**
 * @brief Test for addition of matrices.
 */
TEST_F(MatrixTest, AdditionOperatorGlobalTest) {
  Matrix<int> result = mat1_ + mat2_;
  EXPECT_EQ(result(0, 0), 6);
  EXPECT_EQ(result(0, 1), 8);
  EXPECT_EQ(result(1, 0), 10);
  EXPECT_EQ(result(1, 1), 12);
}

/**
 * @brief Test for the operator() method (const version).
 */
TEST_F(MatrixTest, ConstSubscriptOperatorGlobalTest) {
  const Matrix<int> mat1_const = mat1_;
  EXPECT_EQ(mat1_const(0, 0), 1);
  EXPECT_EQ(mat1_const(0, 1), 2);
  EXPECT_EQ(mat1_const(1, 0), 3);
  EXPECT_EQ(mat1_const(1, 1), 4);
}

/**
 * @brief Test for the polynomials in the matrix.
 */
TEST_F(MatrixTest, PolynomialMatrixTest) {
  EXPECT_EQ(mat4_(0, 0)[0], 3);
  EXPECT_EQ(mat4_(0, 0)[1], 2);
  EXPECT_EQ(mat4_(0, 0)[2], 1);
  EXPECT_EQ(mat5_(0, 0)[0], 3);
  EXPECT_EQ(mat5_(0, 0)[1], 2);
  EXPECT_EQ(mat5_(0, 0)[2], 1);
}

/**
 * @brief Test fot the addition of polynomial matrices.
 */
TEST_F(MatrixTest, PolynomialMatrixAdditionTest) {
  Matrix<Polynomial<int>> result = mat4_ + mat5_;
  EXPECT_EQ(result(0, 0)[0], 6);
  EXPECT_EQ(result(0, 0)[1], 4);
  EXPECT_EQ(result(0, 0)[2], 2);
}

/**
 * @brief Test for the operator<< method.
 */
TEST_F(MatrixTest, PolynomialMatrixOutputStreamOperatorTest) {
  std::stringstream ss;
  ss << mat4_;
  EXPECT_EQ(ss.str(), "([3, 2, 1], [4, 3, 2])\n([0, 0, 0], [0, 0, 0])\n");
}

/**
 * @brief Test for the transpose method.
 */
TEST_F(MatrixTest, TransposeTest) {
  Matrix<int> result = mat1_.GetTranspose();
  EXPECT_EQ(result(0, 0), 1);
  EXPECT_EQ(result(0, 1), 3);
  EXPECT_EQ(result(1, 0), 2);
  EXPECT_EQ(result(1, 1), 4);
}