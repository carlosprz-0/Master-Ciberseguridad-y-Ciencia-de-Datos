/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date August 22 2024
 * @version v0.1.0
 * @brief This file contains the header declaration of the class MessageParser, which is used to parse the messages
 */

#pragma once

#include <vector>
#include <string>

class MessageParser {
 public:
  static std::pair<std::string, int> PadMessage(const std::string& message, int sizeChunk = 32);
  static std::vector<std::string> SplitMessageInChunks(const std::string& message, int sizeChunks = 32);
  static std::string unpad(const std::string& padded_message);
 private:
};