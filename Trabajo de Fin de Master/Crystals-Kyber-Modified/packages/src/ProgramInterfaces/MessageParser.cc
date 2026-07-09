#include "MessageParser.h"

/**
 * @brief This method allows to fill a message so that it has a length multiple of sizeChunk.
 *
 * @param message : The message that is going to be filled.
 * @param sizeChunk : The length that the message should be multiple.
 * @return std::pair<std::string, int> : The resulting message and the padding length.
 */
std::pair<std::string, int> MessageParser::PadMessage(const std::string& message, int sizeChunk) {
  int paddingLength = sizeChunk - (message.length() % sizeChunk);
  if (paddingLength == 0) {
    return {message, 0};
  }
  std::string padding(paddingLength, static_cast<char>(paddingLength));
  return {message + padding, paddingLength};
}

/**
 * @brief This method allows to remove the padding of a message.
 *
 * @param padded_message : The message that is going to be unpadded.
 * @return std::string : The resulting message without padding.
 */
std::string MessageParser::unpad(const std::string& padded_message) {
  unsigned char paddingLength = padded_message.back();
  return padded_message.substr(0, padded_message.length() - paddingLength);
}

/**
 * @brief This method allows to split a message in chunks of size sizeChunks.
 *
 * @param message : The message that is going to be split.
 * @param sizeChunks : The size of the chunks.
 * @return std::vector<std::string> : The resulting vector of strings.
 */
std::vector<std::string> MessageParser::SplitMessageInChunks(const std::string& message, int sizeChunks) {
  std::vector<std::string> blocks;
  for (size_t i = 0; i < message.length(); i += sizeChunks) {
    blocks.push_back(message.substr(i, sizeChunks));
  }
  return blocks;
}