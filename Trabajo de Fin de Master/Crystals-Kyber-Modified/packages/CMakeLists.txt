# Universidad de La Laguna
# Escuela Superior de Ingeniería y Tecnología
# Grado en Ingeniería Informática
# Dependencies:
#   - GTest -> sudo apt install libgtest-dev
#   - OQS   -> https://openquantumsafe.org/liboqs/getting-started.html
#   - OpenSSL -> sudo apt install libssl-dev
#   - Cryptopp -> sudo apt install libcrypto++-dev libcrypto++-doc libcrypto++-utils

# Build process:
#   1. cd <project_folder>
#   2. mkdir build
#   3. cd build
#   4. cmake ..
#   5. make

# Set the minimum Version of CMAKE required
cmake_minimum_required(VERSION 3.14)

# Set the Project configs
project(
  Crystals-Kyber
  VERSION 1.0
  DESCRIPTION "Crystals-Kyber Implementation in C++"
  LANGUAGES C CXX
)

# Debug and release flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# Specify the C++ standard to use
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Locate GTest
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

# Locate OQS library (Open Quantum Safe)
set(CMAKE_PREFIX_PATH "/usr/local/lib/cmake/liboqs")
find_package(liboqs REQUIRED)

# Locate OpenSSL
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})



# Set the location of *.h files
include_directories(${PROJECT_SOURCE_DIR}/src)

# Adds an executable target called "runTests" to be built from the source files listed in the command invocation.
add_executable(
  runTests
  test/gtest_main.cc
  test/Bytes.cc
  test/Matrix.cc
  test/Polynomial.cc
  test/Keccak.cc
  test/NTT.cc
  test/SamplingUnit.cc
  test/EncDecUnit.cc
  test/CompressorUnit.cc
  test/Kyber.cc
  test/Attack.cc
  ./src/DataStructures/Bytes.cc
  ./src/Components/Keccak.cc
  ./src/Components/NTT.cc
  ./src/Components/NTT.h
  ./src/Components/SamplingUnit.cc
  ./src/Components/SamplingUnit.h
  ./src/Components/EncDecUnit.cc
  ./src/Components/EncDecUnit.h
  ./src/Components/CompressorUnit.cc
  ./src/Components/CompressorUnit.h
  ./src/Kyber/Kyber.cc  
  ./src/Kyber/Kyber.h
  ./src/Attack/KleptoKyber.cc
  ./src/Attack/KleptoKyber.h
  ./src/Attack/cyphers/cypher.h
  ./src/Attack/cyphers/mceliece-348864.cc
  ./src/Attack/cyphers/mceliece-348864.h
  ./src/Attack/cyphers/mceliece-460896.cc
  ./src/Attack/cyphers/mceliece-460896.h
  ./src/Attack/cyphers/FRODOKEM-1344-shake.cc
  ./src/Attack/cyphers/FRODOKEM-1344-shake.h
  ./src/Attack/cyphers/FRODOKEM-640-shake.cc
  ./src/Attack/cyphers/FRODOKEM-640-shake.h
  ./src/Attack/cyphers/KYBER-KEM-512.cc
  ./src/Attack/cyphers/KYBER-KEM-512.h
  ./src/Attack/cyphers/ASCON-AEAD128.cc
  ./src/Attack/cyphers/ASCON-AEAD128.h
  ./src/Attack/cyphers/ascon/crypto_aead.c
  ./src/Attack/cyphers/ascon/printstate.c
  ./src/Attack/cyphers/AES-CCM128.cc
  ./src/Attack/cyphers/AES-CCM128.h
)

# Adds an executable target called "client_main" to be built from the source files listed in the command invocation.
add_executable(
  client_main
  ./src/client.cc
  ./src/DataStructures/Polynomial.h
  ./src/DataStructures/Matrix.h
  ./src/DataStructures/Bytes.cc
  ./src/DataStructures/Bytes.h
  ./src/Components/Keccak.cc
  ./src/Components/Keccak.h
  ./src/Components/NTT.cc
  ./src/Components/NTT.h
  ./src/Components/SamplingUnit.cc
  ./src/Components/SamplingUnit.h
  ./src/Components/EncDecUnit.cc
  ./src/Components/EncDecUnit.h
  ./src/Components/CompressorUnit.cc
  ./src/Components/CompressorUnit.h
  ./src/ProgramInterfaces/MessageParser.cc
  ./src/ProgramInterfaces/MessageParser.h
  ./src/Kyber/Kyber.cc  
  ./src/Kyber/Kyber.h
  ./src/ProgramInterfaces/ProgramInterface.cc
  ./src/ProgramInterfaces/ProgramInterface.h
  ./src/Attack/KleptoKyber.cc
  ./src/Attack/KleptoKyber.h
  ./src/Attack/cyphers/cypher.h
  ./src/Attack/cyphers/mceliece-348864.cc
  ./src/Attack/cyphers/mceliece-348864.h
  ./src/Attack/cyphers/mceliece-460896.cc
  ./src/Attack/cyphers/mceliece-460896.h
  ./src/Attack/cyphers/FRODOKEM-1344-shake.cc
  ./src/Attack/cyphers/FRODOKEM-1344-shake.h
  ./src/Attack/cyphers/FRODOKEM-640-shake.cc
  ./src/Attack/cyphers/FRODOKEM-640-shake.h
  ./src/Attack/cyphers/KYBER-KEM-512.cc
  ./src/Attack/cyphers/KYBER-KEM-512.h
  ./src/Attack/cyphers/ASCON-AEAD128.cc
  ./src/Attack/cyphers/ASCON-AEAD128.h
  ./src/Attack/cyphers/ascon/crypto_aead.c
  ./src/Attack/cyphers/ascon/printstate.c
  ./src/Attack/cyphers/AES-CCM128.cc
  ./src/Attack/cyphers/AES-CCM128.h
)

# Add Crypto++ library
link_directories(/usr/include/cryptopp/lib)
target_link_libraries(client_main cryptopp)
target_link_libraries(runTests cryptopp)

# Add OQS library
target_link_libraries(client_main oqs)  # Enlaza el ejecutable con OQS
target_link_libraries(runTests oqs)  # Enlaza las pruebas con OQS

# Add OpenSSL library
target_link_libraries(client_main ${OPENSSL_LIBRARIES})  # Enlaza el ejecutable con OQS
target_link_libraries(runTests ${OPENSSL_LIBRARIES})  # Enlaza las pruebas con OQS



# Add expected compiler features to a target.
target_compile_features(runTests PRIVATE cxx_std_17)
target_link_libraries(runTests ${GTEST_LIBRARIES} pthread)



# Setting app options that the user can enable/disable in the CMake configuration
option(ENABLE_DEBUG "Enable debugging" OFF)
option(ENABLE_TIME "Enable timing measurements" OFF)
option(ENABLE_BENCHMARKING "Enable BENCHMARKING mode" OFF)
option(ENABLE_KEM "Enable KEM mode" OFF)
option(ENABLE_ATTACK "Enable attack mode" OFF)


target_compile_options(runTests PRIVATE)

# If ENABLE_DEBUG is enabled, define the DEBUG macro in the preprocessor
if(ENABLE_DEBUG)
  target_compile_definitions(client_main PRIVATE DEBUG)
endif()

# If ENABLE_TIME is enabled, define the TIME macro in the preprocessor
if(ENABLE_TIME)
  target_compile_definitions(client_main PRIVATE TIME)
endif()

# If ENABLE_BENCHMARKING is enabled, define the BENCHMARKING macro in the preprocessor
if(ENABLE_BENCHMARKING)
  target_compile_definitions(runTests PRIVATE BENCHMARKING)
  target_compile_definitions(client_main PRIVATE BENCHMARKING)
endif()

if(ENABLE_ATTACK)
  target_compile_definitions(client_main PRIVATE ATTACK)
endif()

# If ENABLE_KEM is enabled, define the KEM macro in the preprocessor
if(ENABLE_KEM)
  target_compile_definitions(client_main PRIVATE KEM)
endif()
