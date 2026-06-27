/**
 * Universidad de La Laguna
 * Escuela Superior de Ingeniería y Tecnología
 * Grado en Ingeniería Informática
 * Trabajo de Fin de Grado - Kleptographic Attacks on Lattice-Based CryptoSystems
 *
 * @author Omar Suárez Doro
 * @date August 22 2024
 * @version v0.1.0
 * @brief This is the client program that uses the Kyber cryptosystem
 */

#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <mutex>

#include "./Attack/KleptoKyber.h"
#include "./ProgramInterfaces/ProgramInterface.h"

int main(int argc, char const *argv[]) {
  std::vector<std::string> args(argv, argv + argc);
  ProgramInterface program_interface(args);

  int iters = program_interface.getIterations();
  std::string mode = program_interface.getMode();

  if (mode == "attack") {
    std::cout << "Running the attack:" << std::endl;
    int spec = program_interface.getSpecification();
    if (program_interface.runAttack(spec)) {
      std::cout << "\033[1;32m[+] The backdoor is allways working well.\033[0m" << std::endl;
    } else {
      std::cerr << "\033[1;31m[-] The backdoor is some situations fails.\033[0m" << std::endl;
    }
  } else {
    std::cout << "Running the program in mode: " << mode << std::endl;
    program_interface.run();
  }
  
  return 0;
}