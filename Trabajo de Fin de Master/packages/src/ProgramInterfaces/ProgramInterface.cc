/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date September 5 2024
 * @version v0.1.0
 * @brief This file contains the definition of ProgramInterface methods
 */

#include "./ProgramInterface.h"

#include <iomanip>



/**
 * @brief Construct a new Program Interface:: Program Interface object
 * @param args : The arguments passed to the program 
 */
ProgramInterface::ProgramInterface(const std::vector<std::string>& args) {
  int i = 1;
  while (i < args.size()) {
    if ((args[i] == OPTION_SPECIFICATION_SHORT || args[i] == OPTION_SPECIFICATION_LONG) && i + 1 < args.size()) {
      specification_ = std::stoi(args[++i]);
      ++i;
      continue;
    }
    if ((args[i] == OPTION_MESSAGE_SHORT || args[i] == OPTION_MESSAGE_LONG) && i + 1 < args.size()) {
      input_message_ = args[++i];
      ++i;
      continue;
    }
    if ((args[i] == OPTION_FILE_SHORT || args[i] == OPTION_FILE_LONG) && i + 1 < args.size()) {
      input_file_ = args[++i];
      std::ifstream file(input_file_);
      if (!file.is_open()) {
        throw std::invalid_argument("The file could not be opened");
      }
      std::stringstream buffer;
      buffer << file.rdbuf();
      file.close();
      input_message_ = buffer.str();
      ++i;
      continue;
    }
    if (args[i] == OPTION_HELP_SHORT || args[i] == OPTION_HELP_LONG) {
      std::cout << "\n🔐 Usage: " << args[0] << " [OPTIONS]\n\n";
    
      std::cout << "OPTIONS:\n";
      std::cout << "  " << OPTION_HELP_SHORT << ", " << OPTION_HELP_LONG << "\n"
                << "      Show this help menu\n\n";
    
      std::cout << "  " << OPTION_SPECIFICATION_SHORT << ", " << OPTION_SPECIFICATION_LONG << " <value>\n"
                << "      Kyber/FrodoKEM specification (e.g., 512, 768, 1024)\n\n";
    
      std::cout << "  " << OPTION_CYPHER_BOX_SHORT << ", " << OPTION_CYPHER_BOX_LONG << " <algorithm>\n"
                << "      Select the cypher system:\n"
                << "         - mceliece-348864\n"
                << "         - mceliece-460896\n"
                << "         - frodokem-1344-shake\n"
                << "         - frodokem-640-shake\n"
                << "         - ascon-aead128\n"
                << "         - aes-ccm128\n";
    
      std::cout << "  " << OPTION_MESSAGE_SHORT << ", " << OPTION_MESSAGE_LONG << " <message>\n"
                << "      Message to encrypt (normal mode only)\n\n";
    
      std::cout << "  " << OPTION_FILE_SHORT << ", " << OPTION_FILE_LONG << " <file_path>\n"
                << "      Load message from a text file (normal mode only)\n\n";
    
      std::cout << "  --attack\n"
                << "      Run the program in attack mode\n"
                << "      ⚠️  In this mode:\n"
                << "         - You MUST specify a cypher box using -c / --cypher-box\n"
                << "         - Options like --message and --file will be ignored\n\n";
    
      std::cout << "  --iterations <n>\n"
                << "      Number of iterations to run (default: 1)\n\n";
    
      std::cout << "Examples:\n";
      std::cout << "  " << args[0] << " --spec 512 --message \"Hello\" --cypher-box kyber-kem-512\n";
      std::cout << "  " << args[0] << " --spec 1024 --file ./msg.txt --cypher-box mceliece-348864\n";
      std::cout << "  " << args[0] << " --spec 1024 --cypher-box mceliece-348864 --attack --iterations 10\n\n";
    
      exit(0);
    }
    
    if ((args[i] == OPTION_CYPHER_BOX_SHORT || args[i] == OPTION_CYPHER_BOX_LONG) && i + 1 < args.size()) {
      std::string arg = args[++i];
      if (arg == "mceliece-348864") cypher_box_option_ = MCELIECE_348864;
      else if (arg == "mceliece-460896") cypher_box_option_ = MCELIECE_460896;
      else if (arg == "frodokem-1344-shake") cypher_box_option_ = FRODOKEM_1344_SHAKE;
      else if (arg == "frodokem-640-shake") cypher_box_option_ = FRODOKEM_640_SHAKE;
      else if (arg == "kyber-kem-512") cypher_box_option_ = KYBER_KEM_512_OQS;
      else if (arg == "ascon-aead128") cypher_box_option_ = ASCON_AEAD128;
      else if (arg == "aes-ccm128") cypher_box_option_ = AES_CCM_128;
      ++i;
      continue;
    }
    if (args[i] == "--iterations" && i + 1 < args.size()) {
      iterations_ = std::stoi(args[++i]);
      ++i;
      continue;
    }
    if (args[i] == "--attack") {
      mode_ = "attack";
      ++i;
      continue;
    }
    ++i;
  }
  if (mode_ == "attack") {
    if (cypher_box_option_ == KYBER_CBOX) { cypher_box_option_ = MCELIECE_348864; }  
  }
  
}


/**
 * @brief Run the program
 * @param option : The option to choose the parameters of the Kyber cryptosystem
 * @param seed : The seed to generate the rho and sigma values
 */
void ProgramInterface::run(int option, const std::vector<int>& seed) {
  #ifdef TIME
  std::ofstream out("results.txt");
  #endif

  for (int i = 0; i < iterations_; ++i) {
    // Initialize Kyber object
    std::unique_ptr<Kyber> kyber = std::make_unique<Kyber>(specification_, std::vector<int>(), cypher_box_option_);
    // Pad & Split the message
    std::pair<std::string, int> message_padlen = MessageParser::PadMessage(input_message_);
    std::vector<std::string> message_blocks = MessageParser::SplitMessageInChunks(message_padlen.first);  
    // Start the process
    std::pair<Bytes, Bytes> keypair;
    std::vector<Bytes> cyphertexts(message_blocks.size());
    std::vector<Bytes> decryptedtexts(message_blocks.size());
    
    #ifdef TIME
    auto full_start = std::chrono::high_resolution_clock::now();
    auto keygen_start = std::chrono::high_resolution_clock::now();
    #endif

    #ifndef KEM
    keypair = kyber->KeyGen();
    #ifdef TIME
    auto keygen_end = std::chrono::high_resolution_clock::now();
    auto encrypt_start = std::chrono::high_resolution_clock::now();
    #endif
    EncryptBlocks_(keypair.first, message_blocks, cyphertexts, kyber.get());
    #ifdef TIME
    auto encrypt_end = std::chrono::high_resolution_clock::now();
    auto decrypt_start = std::chrono::high_resolution_clock::now();
    #endif
    DecryptBlocks_(cyphertexts, keypair.second, decryptedtexts, kyber.get());
    auto decrypt_end = std::chrono::high_resolution_clock::now();
    #else
    keypair = kyber->KEMKeyGen();
    #ifdef TIME
    auto keygen_end = std::chrono::high_resolution_clock::now();
    #endif
    // Encapsulate the shared secret
    #ifdef TIME
    auto encrypt_start = std::chrono::high_resolution_clock::now();
    #endif
    std::pair<Bytes, Bytes> pair_ct_shareds = kyber->KEMEncapsulation(keypair.first);
    #ifdef TIME
    auto encrypt_end = std::chrono::high_resolution_clock::now();
    #endif
    KEMEncryptBlocks_(message_blocks, cyphertexts, pair_ct_shareds.second);
    // Decapsulate the shared secret
    #ifdef TIME
    auto decrypt_start = std::chrono::high_resolution_clock::now();
    #endif
    Bytes shared_secret = kyber->KEMDecapsulation(keypair.second, pair_ct_shareds.first);
    #ifdef TIME
    auto decrypt_end = std::chrono::high_resolution_clock::now();
    #endif
    decryptedtexts = cyphertexts;
    if (pair_ct_shareds.second != shared_secret) {
      throw "The shared secret is not the same";
    }
    KEMDecryptBlocks_(cyphertexts, decryptedtexts, shared_secret);
    #endif
    // Decrypt the message
    std::string decrypted_message;
    for (const Bytes& decryptedtext : decryptedtexts) {
      decrypted_message += decryptedtext.FromBytesToAscii();
    }
    if (message_padlen.second > 0) {
      decrypted_message = MessageParser::unpad(decrypted_message);
    }
    std::cout << "Decrypted message: " << decrypted_message << std::endl;
    #ifdef TIME
    std::chrono::duration<double> elapsed = std::chrono::high_resolution_clock::now() - full_start;
    std::cout << "Elapsed time: " << elapsed.count() << "s" << std::endl;
    std::chrono::duration<double> keygen_time = keygen_end - keygen_start;
    std::chrono::duration<double> encrypt_time = encrypt_end - encrypt_start;
    std::chrono::duration<double> decrypt_time = decrypt_end - decrypt_start;
    out << elapsed.count() << " " 
      << keygen_time.count() << " " 
      << encrypt_time.count() << " " 
      << decrypt_time.count() << "\n";
    #endif
  }
}

// Convierte la opción de cifrado seleccionada en un nombre legible.
// Se utiliza para mostrar información por pantalla y para nombrar los CSV de métricas.
std::string ProgramInterface::CypherOptionToString_(int cypher_option) {
  switch (cypher_option) {
    case MCELIECE_348864:
      return "mceliece-348864";
    case MCELIECE_460896:
      return "mceliece-460896";
    case FRODOKEM_1344_SHAKE:
      return "frodokem-1344-shake";
    case FRODOKEM_640_SHAKE:
      return "frodokem-640-shake";
    case KYBER_KEM_512_OQS:
      return "kyber-kem-512";
    case ASCON_AEAD128:
      return "ascon-aead128";
    case AES_CCM_128:
      return "aes-ccm128";
    default:
      return "unknown";
  }
}

/**
 * @brief Run the attack
 * @param option : The option to choose the parameters of the Kyber cryptosystem
 * @param seed : The seed to generate the rho and sigma values
 */
bool ProgramInterface::runAttack(int option, const std::vector<int>& seed) {
    std::string cypher_name = CypherOptionToString_(cypher_box_option_);

    std::cout << "Cypher: " << cypher_name << std::endl; 
    std::cout << "Specification: " << option << std::endl;

    bool not_allways_success = false;    

  // Si se activa ENABLE_TIME, se generan métricas en milisegundos
  // para analizar el rendimiento del ataque con cada mecanismo.
    #ifdef TIME
    std::string output_file =
        "results_attack_" + cypher_name + "_" + std::to_string(option) + ".csv";

    std::ofstream out(output_file);

    out << "iteration,total_ms,setup_ms,backdoor_keygen_ms,recovery_ms,success\n";

    double total_acc_ms = 0.0;
    double setup_acc_ms = 0.0;
    double keygen_acc_ms = 0.0;
    double recovery_acc_ms = 0.0;
    int success_count = 0;
    #endif

  for (int i = 0; i < iterations_; ++i) {
    #ifdef TIME
    auto t_total_start = std::chrono::high_resolution_clock::now();
    auto t_setup_start = std::chrono::high_resolution_clock::now();
    #endif
    // Simulating an attacker installing its klepto key
    Kyber kyber(option, {}, cypher_box_option_);
    std::pair<Bytes, Bytes> attacker_keypair = kyber.KEMKeyGen();

    #ifdef TIME
    auto t_setup_end = std::chrono::high_resolution_clock::now();
    auto t_keygen_start = std::chrono::high_resolution_clock::now();
    #endif
    // Simulating the victim using the klepto system generating the backdoored key
    KleptoKyber klepto_kyber(option, attacker_keypair.first, attacker_keypair.second, {}, cypher_box_option_);
    // The victim generates a key pair
    std::pair<Bytes, Bytes> key_pair_backdoored = klepto_kyber.RunBackdoor();

    #ifdef TIME
    auto t_keygen_end = std::chrono::high_resolution_clock::now();
    auto t_recover_start = std::chrono::high_resolution_clock::now();
    #endif
    // Simulating the attacker recovering the victim's secret key using its secret key
    bool success = klepto_kyber.recoverSecretKey(key_pair_backdoored.first) == key_pair_backdoored.second;

    #ifdef TIME
    auto t_recover_end = std::chrono::high_resolution_clock::now();
    auto t_total_end = t_recover_end;

    double setup_time_ms =
        std::chrono::duration<double, std::milli>(t_setup_end - t_setup_start).count();

    double keygen_time_ms =
        std::chrono::duration<double, std::milli>(t_keygen_end - t_keygen_start).count();

    double recovery_time_ms =
        std::chrono::duration<double, std::milli>(t_recover_end - t_recover_start).count();

    double total_time_ms =
        std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();

    total_acc_ms += total_time_ms;
    setup_acc_ms += setup_time_ms;
    keygen_acc_ms += keygen_time_ms;
    recovery_acc_ms += recovery_time_ms;

    if (success) {
      ++success_count;
    }

    out << i << ","
        << std::fixed << std::setprecision(6)
        << total_time_ms << ","
        << setup_time_ms << ","
        << keygen_time_ms << ","
        << recovery_time_ms << ","
        << (success ? 1 : 0) << "\n";
    #endif

    if (!success) {
      std::cerr << "The backdoor is not working well" << std::endl;
      not_allways_success = true;
    }
  }

    #ifdef TIME
  out.close();

  std::cout << "\nTiming summary\n";
  std::cout << "Output file: results_attack_" << cypher_name << "_" << option << ".csv\n";
  std::cout << "Iterations: " << iterations_ << "\n";
  std::cout << "Success rate: " << success_count << "/" << iterations_ << "\n";
  std::cout << std::fixed << std::setprecision(6);
  std::cout << "Average total time: " << total_acc_ms / iterations_ << " ms\n";
  std::cout << "Average setup time: " << setup_acc_ms / iterations_ << " ms\n";
  std::cout << "Average backdoor keygen time: " << keygen_acc_ms / iterations_ << " ms\n";
  std::cout << "Average recovery time: " << recovery_acc_ms / iterations_ << " ms\n";
  #endif

  return !not_allways_success; 
}



/**
 * @brief Encrypt the blocks of the message
 * @param public_key : The public key
 * @param message_chunks : The vector of message chunks
 * @param ciphertexts : The vector of ciphertexts
 * @param kyber : The Kyber object
 */
void ProgramInterface::EncryptBlocks_(const Bytes& public_key, const std::vector<std::string>& message_chunks, std::vector<Bytes>& ciphertexts, Kyber* kyber) {
  // Generate a 32 byte coins (0 or 1)
  Bytes coins = Bytes();
  for (int i = 0; i < 32; ++i) {
    coins += Bytes(std::vector<int>{rand() % 2});
  }
  std::vector<std::thread> threads;
  for (int i = 0; i < message_chunks.size(); ++i) {
    threads.push_back(std::thread(ProcessBlock, i, Bytes(message_chunks[i]), public_key, coins, std::ref(ciphertexts), kyber));
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
}

/**
 * @brief Decrypt the blocks of the message
 * @param ciphertexts : The vector of ciphertexts
 * @param secret_key : The secret key
 * @param decryptedtexts : The vector of decrypted texts
 * @param kyber : The Kyber object
 */
void ProgramInterface::DecryptBlocks_(const std::vector<Bytes>& ciphertexts, const Bytes& secret_key, std::vector<Bytes>& decryptedtexts, Kyber* kyber) {
  std::vector<std::thread> threads;
  for (int i = 0; i < ciphertexts.size(); ++i) {
    threads.push_back(std::thread(ProcessDecryptionBlock, i, ciphertexts[i], secret_key, std::ref(decryptedtexts), kyber));
  }
  for (std::thread& thread : threads) {
    thread.join();
  }
}

/**
 * @brief Process a block of the message
 * @param id : The id of the block
 * @param block : The block to process
 * @param key : The key to use
 * @param coins : The coins to use
 * @param ciphertexts : The vector of ciphertexts
 * @param kyber : The Kyber object
 */
void ProgramInterface::ProcessBlock(int id, const Bytes& block, const Bytes& key, const Bytes& coins, std::vector<Bytes>& ciphertexts, Kyber* kyber) {
  ciphertexts[id] = kyber->Encryption(key, block, coins);
}

/**
 * @brief Process the decryption of a block
 * @param id : The id of the block
 * @param block : The block to decrypt
 * @param key : The key to use
 * @param decryptedtexts : The vector of decrypted texts
 * @param kyber : The Kyber object
 */
void ProgramInterface::ProcessDecryptionBlock(int id, const Bytes& block, const Bytes& key, std::vector<Bytes>& decryptedtexts, Kyber* kyber) {
  decryptedtexts[id] = kyber->Decryption(key, block);
}

/**
 * @brief Encrypt the blocks of the message using the KEM
 * @param message_chunks : The vector of message chunks
 * @param operand : The vector of operands
 * @param key : The key to use
 */
void ProgramInterface::KEMEncryptBlocks_(const std::vector<std::string>& message_chunks, std::vector<Bytes>& operand, const Bytes& key) {
  for (int i = 0; i < operand.size(); ++i) {
    operand[i] = Bytes(message_chunks[i]) ^ key; 
  }
}

/**
 * @brief Decrypt the blocks of the message using the KEM
 * @param encrypted_messages : The vector of encrypted messages
 * @param operand : The vector of operands
 * @param key : The key to use
 */
void ProgramInterface::KEMDecryptBlocks_(const std::vector<Bytes>& encrypted_messages, std::vector<Bytes>& operand, const Bytes& key) {
  for (int i = 0; i < operand.size(); ++i) {
    operand[i] = encrypted_messages[i] ^ key; 
  }
}

