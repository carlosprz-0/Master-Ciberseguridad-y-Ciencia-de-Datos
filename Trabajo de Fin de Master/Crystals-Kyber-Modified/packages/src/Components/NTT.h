/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date July 18 2024
 * @version v0.1.0
 * @brief This file contains the header declaration of the class NTT, which give us 
 * The Number Theorical Transformation.
 */

#pragma once

#include <stdexcept>

#include "../DataStructures/Matrix.h"
#include "../DataStructures/Bytes.h"

#define FIRST_PRIMITIVE_ROOT_3329 17
#define MODULUS_3329 3329

class NTT {
 public:
  NTT(int n, int q) { n_ = n; q_ = q; }

  // Getters
  int GetN() { return n_; }
  int GetQ() { return q_; }
  // Setters
  void SetN(int n) { n_ = n; }
  void SetQ(int q) { q_ = q; }

  Polynomial<int> NTT_Kyber(const Polynomial<int>& kPolynomial, bool is_ntt) const;
  Polynomial<int> NTT_(const Polynomial<int>& kPolynomial) const;
  Polynomial<int> INTT_(const Polynomial<int>& kPolynomial) const;
  Polynomial<int> ParsePolynomial_(const Bytes& kBytes) const;

  
  int FirstPrimitiveRoot_(int n) const;
  int BitReverse_(int n, int bits) const;
  int PowerWithMod_(int base, int exp, int mod) const;

  Matrix<Polynomial<int>> GenerateMatrix_(int k_size, Bytes rho, bool traspose = false) const;
  Matrix<Polynomial<int>> multMatrixViaNTT(const Matrix<Polynomial<int>>& A, const Matrix<Polynomial<int>>& B);
  Polynomial<int> pointwise_(const Polynomial<int>& p, const Polynomial<int>& g);

 private:
  int  n_ = 256;
  int  q_ = 3389;
  bool _IsPrime(int n) const;
};
