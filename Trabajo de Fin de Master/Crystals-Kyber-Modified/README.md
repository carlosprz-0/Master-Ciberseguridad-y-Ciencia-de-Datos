# Authors:
- **Omar Suárez Doro** (alu0101483474@ull.edu.es)
- **Edgar Pérez Ramos** (alu0101207667@ull.edu.es)
- **Pino Caballero Gil** (pcaballe@ull.edu.es)


## Acknowledgments

I would like to express my deepest gratitude to those who contributed to the development and success of this project:

- **Prassana Ravi**, the original author whose groundbreaking work on kleptographic attacks inspired this research and implementation. His contributions to the field of cryptography have been invaluable.

- **Dr. Pino Caballero Gil**, my TFG supervisor, for her guidance, support, and encouragement throughout the development of this project. Her expertise and insights have been instrumental in shaping the direction and depth of this research.

- **Edgar Pérez Ramos**, my co-supervisor, for his invaluable technical advice, feedback, and patience. His support and mentorship were crucial in overcoming challenges and achieving the project objectives.

This work would not have been possible without their contributions, and I am sincerely grateful for their time, knowledge, and dedication.


## 1. Index
- [1. Index](#1-index)
- [2. Abstract](#2-abstract)
- [3. Why C++?](#3-why-c)
- [4. Main Concepts](#4-main-concepts)
  - [a) Lattices](#a-lattices)
  - [b) Learning With Errors Problem](#b-learning-with-errors-problem)
  - [c) Polynomial Ring](#c-polynomial-ring)
  - [d) Finite Field](#d-finite-field)
- [5. Parameters and Specifications](#5-parameters-and-specifications)
- [6. Components](#6-components)
  - [a) Data Structures](#a-data-structures)
  - [b) Keccak](#b-keccak)
  - [c) Sampling Unit](#c-sampling-unit)
  - [d) Number Theoretic Transform (NTT)](#d-number-theoretic-transform-ntt)
  - [e) EncDecUnit Class](#e-encdecunit-class)
  - [f) Compression Unit](#f-compression-unit)
  - [g) Kyber Class](#f-kyber-class)
- [7. Introduction to the Attack](#7-introduction-to-the-attack)
  - [7.1 Key Aspects Before Implementation](#71-key-aspects-before-implementation)
- [8. Deployment and Compilation](#8-deployment-and-compilation)
  - [8.1 Deployment](#deployment)
  - [8.2 Compilation](#compilation)
  - [8.3 Compilation Flags](#compilation-flags)
  - [8.4 Client Parameters](#client-parameters)
- [9. Advantages Over the Original Repository](#10-advantages-over-the-original-repository)
- [10. Results](#11-results)
  - [10.1 Timing Calculated for McEliece-348864](#timing-calculated-for-mceliece-348864)
  - [10.2 Timing Calculated for McEliece-460896](#timing-calculated-for-mceliece-460896)
  - [10.3 Comparison of Malicious KeyGen Timings](#comparison-of-malicious-keygen-timings)



## 2. Abstract
This repository contains a C++ implementation of **Crystals Kyber Algorithm**(CKA) and kleptographic attack on this algorithm. The CKA is a standard in **Post Quantum Cryptography**(PQC), which development is driven by National Institute of Standards and Technology ([NIST](https://www.nist.gov/)). The Reference repository of the attack is from Prassana Ravi ([PRASANNA-RAVI/Klepto_on_Kyber](https://github.com/PRASANNA-RAVI/Klepto_on_Kyber)).


## 3. Why C++?
The main purpouse of this repository is to document the CKA in a **didactic way**. Despite the fact that C++ is a language which learning curve is really steep, it provides a lot of features that may help by the time you are learning how Kyber works. One of this features is the **Object Oriented Programming**(POO), that allow us to understand kyber as some pieces or components that work together to bring us Well Developed cryptographyc scheme.

## 4. Main Concepts

The first thing we need to do, in order to understand Kyber is learning about **Mathematical fundamentals** that Kyber is based (don't be afraid, I am bad at Maths too). In cryptography, we need problems that are easy to solve for the device we are communication with, and hard for others, for example, attackers that are sniffing the traffic over the network where messages are being sent. Crystals Kyber Algorithm uses mathematical structures known as **lattices**, and the problem that we are going to use is the Learning With Errors Problem

### a) Lattices
A lattice is a mathematical structure which is created using integer lineal combinations  of a **vectorial base**. This means that having two vectors $\vec{a}$ and $\vec{u}$ and making some lineal combinations like ($2 \cdot \vec{a} + 1 \cdot \vec{u}$) or ($2 \cdot \vec{u}$) we can generate a lattice. Here is a visual way:

<div align="center"><img src="https://upload.wikimedia.org/wikipedia/commons/c/c3/Bravais_Lattices.gif" height=200px /> </div>
<br>

> [!note]
> It is important to keep in mind that **different vectors** can generate the **same** lattice
>

### b) Learning With Errors Problem
Suppose that somehow we can represent messages in the points of the lattice that we are going to use and we have a *equations system* that we need to solve, in order to get the key, and therefore, decypher the encrypted message. The solution of this system is the exact lineal combination to get the point that represent the message. This sounds weird, because seems like bruteforce mixed with Gauss Method, substitution could break this scheme.
<br>

<div align="center"> <img src="https://latex.codecogs.com/svg.image?\inline&space;\LARGE&space;\bg{white}{\color{White}\begin{eqnarray}2x&plus;y=1\\x&plus;y=4\\x&plus;y&plus;z=6\end{eqnarray}}" title="{\color{White}\begin{eqnarray}2x+y=1\\x+y=4\\x+y+z=6\end{eqnarray}}" /> </div>

<br>

The answer is **YES**. That is the reason that we are going to introduce a little "noise" or "error" in the system:

<br>

<div align="center"> <img src="https://latex.codecogs.com/svg.image?\inline&space;\LARGE&space;\bg{white}{\color{White}\begin{eqnarray}2x&plus;y=1-{\color{red}1}\\x&plus;y=4-{\color{red}3}\\x&plus;y&plus;z=6-{\color{red}2}\end{eqnarray}}" title="{\color{White}\begin{eqnarray}2x+y=1-{\color{red}1}\\x+y=4-{\color{red}3}\\x+y+z=6-{\color{red}2}\end{eqnarray}}" /> </div>

<br>

The change is simple, but the problem is a way **different**. The problem now consists in guessing the closest point to the solution. The thing here is that if we know the secret key, that could be compared to having a good vectorial base, solve this problem is a way easier than didn't have it, which means having a bad base.


### c) Polynomial ring
Crystals Kyber Algorithm use numbers contained in a polynomial ring, which allow us to work with modular operations, enhancing memory space needed and working in a finite field:

<div align="center"> <img src="https://latex.codecogs.com/svg.image?\inline&space;\LARGE&space;\bg{white}{\color{White}\{\mathbb{Z}_q[x]/(x^n&plus;1)\}}" title="{\color{White}\{\mathbb{Z}_q[x]/(x^n+1)\}}" /> </div>
<br>

Which basically means numbers contained in the interval of module operation.
Here it's a table that resume the main concepts that you should keep in mind:

| Concept | Short Description |
| ------- | ----------------- |
| Lattice | A lattice is a mathematic structure, which is built by lineal combinations of a n -vectors, where n define the dimention of the lattice. |
|LWE problem | The **learning with errors** problem consist in, having a vectorial base and a point (P) in the plane. Discover which lineal combination of the vectors should we use, in order to be in the closest point to P.
|Big O notation (**O(n)**)| Is a standard which function describe the time that an algorithm needs to be completed. This notation focussed in the **worst case** with a sample of n elements.|
 
 <br>

### d) Finite field
A Finite field, also known as *Body*, is a finite set of elements on which algebraic operations such as addition, subtraction, multiplication, and division (except for zero) are defined, which fulfill certain properties:

- Finite number of elements: A finite body contains a finite number of elements, generally denoted as $\mathbb{F} _q$ , where  q is the number of elements in the field.
- Modular Arithmetic: Operations in a finite body often use modular arithmetic, are performed in modulus q, where q is the prime established.
- Properties of operations: The addition, subtraction, multiplication and division (except for 0) are well defined operations and fulfill the associative, commutative, distributive properties and have neutral elements.

<br>

## 5. Parameters and Specifications

Crystals Kyber Algorithm needs a set of parameters, here it is an explanation of each one:
| Parameter | In code | Description |
| --- | --- | --- |
| n | n | The grade of the polynomials that we will use (the size).|
| q | q | The prime that we will use to make the module.|
| k | k | The size of the matrix.|
| $\eta_1$, $\eta_2$ | n1, n2 | The parameters that we will use to generate the random distribution. |
| du, dv | du, dv | The parameters of compressions, in order to reduce the size of the keys and cyphered|

Once we know about the parameters, we can talk about the differents specifications of Crystals Kyber:

| Specification | NIST Security Level | Compared to | n | k | q | Size secret-key(bytes) | Size public-key(bytes) | Size cyphered-text(bytes) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Kyber512 | 1 |  RSA-2048 o AES-128 | 256 | 2 | 3329 | 1632 | 800 | 768 |
| Kyber768 | 3 |  RSA-3072 o AES-192 | 256 | 3 | 3329 | 2400 | 1184 | 1088 |
| Kyber1024 | 4 |  RSA-4096 o AES-256 | 256 | 4 | 3329 | 3168 | 1568 | 1568 |


### Why do we select this numbers?
- The **n(256)** and **q(3329)** numbers are selected in order to equilibrate between security and efficiency. Notice that the prime is close to a power of 2. being easier the calculations.
- The $\eta$ value (2) keeps the value low, because we want to have a little error, and recuperate the original point.

## 6. Components

### a) Data Structures
Our implementation of CKA is based in two **primitive** structures, which hold the main data.

- **Polynomial**: This class represents a polynomial and provide us with some useful functionalities such as the *addition*, *print_operation*, etc.. It is mapped using a **vector structure** of T, where T is a template parameter.

Example of use:
```c++
Polynomial<int> poly1_ = Polynomial<int>(3);
Polynomial<int> poly2_ = Polynomial<int>(3);
    poly1_[0] = 1;
    poly2_[1] = 5;
Polynomial<int> result = poly1_ + poly2_; // [6, 0, 0]
```
<br>
<br>

- **Matrix**: This class represents a matrix of T type, which T is the template parameter, as the previous one. This class follows a desing pattern called **Facade** in order to make easy to work with a 3 dimensional vectors structure.

Example of use:
```c++
Matrix<Polynomial<int>> mat4_ = Matrix<Polynomial<int>>(2, 2, 3);
Matrix<Polynomial<int>> mat5_ = Matrix<Polynomial<int>>(2, 2, 3);
    Polynomial<int> p1 = Polynomial<int>(3);
    // Initialize the values
    Polynomial<int> p2 = Polynomial<int>(3);
    // Initialize the values
    mat4_(0, 1) = p2;
    mat5_(0, 0) = p1;
Matrix<Polynomial<int>> result = mat4_ + mat5_;
```

<br>
<br>


- **Bytes**: This class implements a set of bytes. They are mapped as `std::vector<unsigned char>` object, this is because we have the entire **8 bits** range representation. If we have used normal char 1 bit is used for sign representation, this is not a real problem in representation operations, but it is **highly** important in arithmethic operations. If we want to operate in a signed way, we can use the signed version of methods. The main reason of using our own **Byte Structure** is to follow facade patterns, in order to simplify the management of some custom **data structures** that libraries like [Cryptopp](https://github.com/weidai11/cryptopp) use.

The bytes structure work with each bytes as **Little Endian** way, that means that the left Byte is the less significant. However, bits are interpreted from right to left (the common way to read binary).



The bytes structure allow us to make operations with Bytes in a simple way. Hereis a list of some methods that are included in this class:

| **Method**                      | **Description**                                                                                                                                                                |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `operator+`                      | Performs concatenation of byte sequences.                                                                                                                                      |
| Bitwise operations               | Supports bitwise operations such as `and (&)`, `or (|)`, and `not (~)`.                                                                                                       |
| `operator<<` and `operator>>`    | Performs a shift operation on each byte in the string. **CAREFUL**: Does not manage carry bits; overflowed bits are discarded. **TAKE CARE OF THIS**.                          |
| **Transformation Methods**       | **Description**                                                                                                                                                               |
| `FromBytesToBits`                | Returns a string with the bit representation of each byte.                                                                                                                    |
| `FromBytesToHex`                 | Returns a string with the hex representation of each byte.                                                                                                                    |
| `FromBytesToNumbers`             | Returns a long integer representing the value of the bytes by iterating over each bit and adding `bit * 2^n` to the result.                                                   |
| `GetBytesAsNumbersVector`        | Returns a vector of integers, each representing the individual value of each byte.                                                                                            |

<br>

### b) Keccak
The Keccak component consists in a set of **Cryptographic functions** which are provided by the [Cryptopp](https://github.com/weidai11/cryptopp) library, also known as Crypto++. Keccak is structured by static methods (this is because, in our opinion, it is easy to understand the kyber as boxes that implements a set of functions that we will need) that we will refer to them, for simplicity, **"Logic Gates"**.

This Logic Gates operates with privates methods which use the functions of the library Crytopp and return our custom `Byte` structure. This logic gates are:

|Gate | Description |
| --- | --- |
|Extendable-output function (**XOF**)| Is an extension of the cryptographic hash that allows its output to be arbitrarily long.|
|Pseudorandom function family(**PRF**)|Is a collection of efficiently-computable functions which emulate a random oracle in the following way: no efficient algorithm can distinguish (with significant advantage) between a function chosen randomly from the PRF family and a random oracle (a function whose outputs are fixed completely at random).|
|Key derivation function (**KDF**) | Is a cryptographic algorithm that derives one or more secret keys from a secret value such as a master key, a password, or a passphrase using a pseudorandom function|
|(**H**) |Hash function used to derive cryptographic material (seed for public matrix A, coins, shared keys) from arbitrary-length byte strings. Output is a 32-byte string.|
|(**G**)|Hash function used to generate noise polynomials for the secret key. Output is a concatenation of two 32-byte strings.|

<br>

### c) Sampling Unit

The sampling unit is a component that generates noise samples
(the errors involved in the Learning With Errors or LWE problem) and also
generates random polynomials that comply with the scheme's security. To
ensure the algorithm works correctly, a distribution close to 0 is introduced
so that the error is small, allowing the recovery of the original data.

To achieve this, Kyber uses the Central Binomial Distribution (CBD),
which provides:

- Precise control of the generated noise.
- Computational efficiency (less complex operations).
- Guarantee of the scheme's security.

This component has two main methods:

1. **GenerateDistributionMatrix**: Generates a \(k \times 1\) matrix using the CBD. It should be noted that through a PRF gate prior to the CBD,
   the output's determinism is ensured, biases in distribution generation are
   prevented, and resistance to attacks is reinforced.

2. **CBD**: Implements the central binomial distribution. In the code, the inversion
   in byte management ensures compatibility with internal representations
   and cryptographic standards.


> [!NOTE]
> 
> ### Why do we use a Binomial distribution instead of a Normal(Gaussian) Distribution?
>
> First, we need to consider that the Binomial distribution is discrete instead of continuous, this is convenient because we need to generate 0s or 1s, we are not interested in values like 0.9.
> But we also consider: 
> - The simplicity of implementation
> - The accuracy in noice management


### d) Number Theoretic Transform (NTT)

The Number Theoretic Transform (NTT) is a variant of the Discrete
Fourier Transform (DFT), but it operates in a finite field instead of complex
numbers. As mentioned earlier, this element enables efficient computations
with integers, which is crucial for cryptographic applications. The main reason
to use NTT in cryptographic schemes like Kyber is:

- Improve the efficiency of polynomial multiplication.
- Reduce computational complexity.
- Ensure security by working within a finite field.

It is worth noting that in the repository's implementation, the
NTT class includes methods to perform polynomial multiplication using pointwise multiplication, which involves:

1. **Transformation to the frequency domain**: Each polynomial is transformed
   using the NTT, converting convolution operations in the time domain into
   pointwise multiplications in the frequency domain.

2. **Pointwise multiplication**: Once in the frequency domain, the coefficients
   of the polynomials are directly multiplied with each other.

3. **Inverse Transformation (INTT)**: Finally, the inverse NTT transformation
   is applied to return the result to the time domain, obtaining the final product polynomial.


> [!Important]
>
> ### Complexity Decreased
> ---
> 
>The computational complexity is highly decreased due to this algorithm:
> <div align="center"> <img src="https://latex.codecogs.com/svg.image?\inline&space;\LARGE&space;\bg{white}{\color{White}O(n^2)\rightarrow&space;O(n\cdot&space;log(n))}" title="{\color{White}O(n^2)\rightarrow O(n\cdot log(n))}" /> </div>
>This happens because the following domain transformation is performed:
> <div align="center"> <img src="https://latex.codecogs.com/svg.image?\inline&space;\LARGE&space;\bg{white}{\color{White}R_q^{k\cdot&space;k}\rightarrow&space;R_q^k}" title="{\color{White}R_q^{k\cdot k}\rightarrow R_q^k}" /> </div>
> Where **q** is the prime that we are using in the algorithm and **k** the dimension.
> <div align="center"> <img src="https://latex.codecogs.com/svg.image?\inline&space;\LARGE&space;\bg{white}{\color{White}\begin{pmatrix}\begin{bmatrix}1&2&3\end{bmatrix}&\begin{bmatrix}1&2&3\end{bmatrix}\\\begin{bmatrix}1&2&3\end{bmatrix}&\begin{bmatrix}1&2&3\end{bmatrix}\end{pmatrix}_{R{^{k\cdot&space;k}_q}}\\\longrightarrow\begin{pmatrix}\begin{bmatrix}165\end{bmatrix}\\\begin{bmatrix}125\end{bmatrix}\\\end{pmatrix}_{R_q^k}}" title="{\color{White}\begin{pmatrix}\begin{bmatrix}1&2&3\end{bmatrix}&\begin{bmatrix}1&2&3\end{bmatrix}\\\begin{bmatrix}1&2&3\end{bmatrix}&\begin{bmatrix}1&2&3\end{bmatrix}\end{pmatrix}_{R{^{k\cdot&space;k}_q}}\\\longrightarrow\begin{pmatrix}\begin{bmatrix}165\end{bmatrix}\\\begin{bmatrix}125\end{bmatrix}\\\end{pmatrix}_{R_q^k}}" /> </div>
>
> ### How does it works?
> ---
> NTT takes a polynomial with coefficients in a **finite body** (explained at [d) Finite Field](#d-finite-field)), transform it into a way that makes **point-to-point** highly increasing the performance of the polynomial multiplication. We will see in PWMUnit section that this trasformations will make **pointwise multiplication** be a strong ally.
> <div align="center"> <img src="https://latex.codecogs.com/svg.image?{\color{White}x(k)=\sum_{j=0}^{n-1}x(j)\cdot\omega^{j\cdot%20k}\(mod\;q\)}" title="cosita" /> </div>



### e) EncDecUnit Class

The **EncDecUnit** class is a component whose main purpose is to handle the encoding and decoding of messages within the cryptographic scheme.

The **EncDecUnit** class is responsible for:

- Transforming matrices of polynomials into byte sequences.
- Performing the inverse operation, converting byte sequences back into polynomial matrices.

This functionality is essential to efficiently represent data and ensure it can be transmitted or stored without losing information.

### f) Compression Unit

The purpose of the **CompressorUnit** class is to define functions for compression and decompression, enabling the removal of some less significant bits from the ciphertext. This does not affect the validity of the ciphertext or its subsequent decryption, thereby reducing the size of encrypted texts.

#### Main Methods

1. **CompressMatrix**: Compresses a matrix of polynomials by reducing the number of bits per coefficient, resulting in a more compact representation.
2. **DecompressMatrix**: Performs the inverse operation of `CompressMatrix`, restoring the compressed data to its original form with the specified precision.
3. **Compress**: Compresses a polynomial using a defined number of bits per coefficient.
4. **Decompress**: Decompresses a previously compressed polynomial to recover its original form.
5. **round up**: Performs rounding up of a floating-point number with a precision adjustment to avoid calculation errors.


### f) Kyber Class

The **Kyber** class is the main element of the entire scheme. This class handles the instantiation and orchestration of the other component classes. There are 6 main methods, the first 3 correspond to the CPAKE mode, while the following 3 are respective to the CCAKEM mode.

#### Main Methods

| Algorithm           | Code Name                   | Description         |
|---------------------|-----------------------------|---------------------|
| Key Generation      | Kyber.CPAKE.KeyGen          | Keygen              |
| Encryption          | Kyber.CPAKE.Enc             | Encryption          |
| Decryption          | Kyber.CPAKE.Dec             | Decryption          |
| Key Generation      | Kyber.CCAKEM.KeyGen         | KEMKeyGen           |
| Encapsulation       | Kyber.CCAKEM.Enc            | KEMEncapsulation    |
| Decapsulation       | Kyber.CCAKEM.Dec            | KEMDecapsulation    |


## 7. Introduction to the Attack

This work focuses on the analysis and implementation of a kleptographic attack described by Ravi in ?. This attack proposes the insertion of a backdoor into post-quantum encryption schemes, specifically in Kyber KEM, exploiting inherent properties of the Module-LWE problem.

The described attack modifies the key generation procedure in the KEM mode so that the generated public keys leak information about the seed (the 32-byte vector) used to create the secret. The objective is to enable the generation of the same secret key from the same recovered seed.

To execute the attack, it is divided into 3 main phases:

1. **Insertion of attacker keys into the cryptosystem (Offline)**:  
   In this phase, the attacker generates their keys using an encryption algorithm, such as Mceliece-348864, and then embeds these keys into the cryptosystem to make the necessary modifications. It is important to note that the key generation must use the same encryption algorithm employed in the communications intended to be compromised.

2. **Execution of the modified KeyGen (Online)**:  
   In this phase, the modified `KeyGen` method is executed, generating keys that allow secure communications. However, the public key contains information that enables the recovery of the seed.

3. **Seed recovery (Offline)**:  
   The goal of this phase is for the attacker to recover the seed from the target's public key.

### Key Aspects Before learn

Before proceeding with the implementation, there are several key aspects to consider.

In the code, a child class named **KleptoKyber** has been created. This is because, during the second phase, methods from the **Kyber** base class are used, leveraging its structure to facilitate development.

Additionally, to compare results and implement the attack using the encryption proposed in Ravi's paper, a hierarchy of encryption classes has been designed. In this hierarchy, the base class is **Cypher**, which implements the **Facade** pattern. This pattern allows the class to invoke methods from the [Open Quantum Safe](https://openquantumsafe.org/) library in C++, simplifying interaction with the underlying algorithms.

The base class contains a private attribute called `cypher_`, which is an object of the **Cypher** class. This attribute is used to perform operations such as encryption, key generation, or other related actions, using the specified encryption instead of the default behavior, thereby ensuring greater flexibility in cryptographic operations.

In the official repository of the attack, the effectiveness of this approach has been demonstrated specifically for the **Kyber768** specification, as well as with the **mceliece-460896** algorithm. This supports the functionality and viability of the implementation in different scenarios.

To ensure the proper replication of the attack and its correct functionality:

- **Docker** has been used as the execution environment.
- Specific tests have been implemented to verify the attack's functionality with each encryption and specification, increasing development reliability.


## 8. Deployment and Compilation

### Deployment

To deploy the code, Docker can be used. By running the command `docker compose up`, a container will be deployed using the `ubuntu@latest` image. This container mounts a volume from the host system into the `packages` folder. This setup allows code modifications within the container to persist. Although mounting a volume presents certain risks, once the final version is ready, the folder could simply be copied instead of relying on volumes.

### Compilation

To compile the project, execute the following commands in the root folder of the project:

```bash
mkdir build
cd build
cmake .. (Here you can specify compilation flags)
make
```

### Compilation Flags

The project supports several compilation flags:

| **Compilation Flag**     | **Description**                                  |
|--------------------------|--------------------------------------------------|
| `-DENABLE_KEM`           | Enables CCAKEM mode.                             |
| `-DENABLE_DEBUG`         | Displays additional debug messages.              |
| `-DENABLE_TIME`          | Measures the execution time of processes.        |

These commands will generate two executables: `runTests` and `client_main`.  
The first is used to execute all project tests, while the second is a client program for encryption or attack simulations.

---

### Client Parameters

```text
🔐 Usage: ./client_main [OPTIONS]

OPTIONS:
  -h, --help
      Show this help menu

  -s, --spec <value>
      Kyber/FrodoKEM specification (e.g., 512, 768, 1024)

  -c, --cypher-box <algorithm>
      Select the cypher system:
         - mceliece-348864
         - mceliece-460896
         - frodokem-1344-shake
         - frodokem-640-shake


  -m, --message <message>
      Message to encrypt (normal mode only)

  -f, --file <file_path>
      Load message from a text file (normal mode only)

  --attack
      Run the program in attack mode
      ⚠️  In this mode:
         - You MUST specify a cypher box using -c / --cypher-box
         - Options like --message and --file will be ignored

  --iterations <n>
      Number of iterations to run (default: 1)

Examples:
  ./client_main --spec 512 --message "Hello" --cypher-box kyber-kem-512
  ./client_main --spec 1024 --file ./msg.txt --cypher-box mceliece-348864
  ./client_main --spec 1024 --cypher-box mceliece-348864 --attack --iterations 10
```

## 10. Advantages Over the Original Repository

The first thing to consider is the approach taken by the author of the original code. Ravi prioritized efficiency over code expressiveness and traceability. This resulted in highly efficient code in terms of runtime but sacrificed the ability to understand the attack easily or trace its execution. This makes it extremely difficult for users unfamiliar with kleptographic attack ecosystems to follow the code, particularly due to the use of numerous impure functions (evident in the direct manipulation of structures rather than returning the results of functions). Combined with minimal documentation and the naming conventions used, this makes the code extremely hard to follow. However, in the proposed implementation, best practices were followed to avoid this problem.

Another issue to highlight is the inconsistency of the installation and replication guide provided by the author in the `README.md`. It not only excludes non-expert users but also fails to work for certain systems, as the package proposed for dependency installation does not function properly. To address this issue, our attack can be deployed using Docker and Docker Compose, enabling it to run on any system equipped with container management software.

Another aspect to consider is the absence of a client program for user-driven testing in the original repository; it only includes a test file. In the proposed implementation, a client program is included, which, despite being somewhat static, has an associated class `ProgramInterface` that accounts for scenarios involving parallel computation.

Finally, it is important to note that in the official repository, the author only considered implementing the attack for the Kyber768 specification and the mceliece-460896 encryption algorithm. The proposed implementation supports all Kyber specifications and any encryption scheme where the computation of `c` is less than 12. Therefore, certain schemes like FRODOKEM, where the backdoor does not work, are excluded. Tests developed in the `Attack.cc` file within the `tests` folder can be consulted to confirm that some tests expect the backdoor to fail.



## 11. Results
### Timing Calculated for McEliece-348864

| **Kyber Spec**                | **Complete**       | **Keygen**         | **Enc.**           | **Dec.**           |
|-------------------------------|--------------------|--------------------|--------------------|--------------------|
| Kyber512 + mceliece348864     | 1.84 ms           | 0.83 ms           | 0.88 ms           | 0.13 ms           |
| Kyber768 + mceliece348864     | 1.86 ms           | 0.85 ms           | 0.88 ms           | 0.13 ms           |
| Kyber1024 + mceliece348864    | 1.87 ms           | 0.86 ms           | 0.87 ms           | 0.14 ms           |
| Kyber512 + M.mceliece348864   | 5.17 ms           | 4.16 ms           | 0.88 ms           | 0.13 ms           |
| Kyber768 + M.mceliece348864   | 6.56 ms           | 5.56 ms           | 0.88 ms           | 0.13 ms           |
| Kyber1024 + M.mceliece348864  | 8.26 ms           | 7.26 ms           | 0.87 ms           | 0.14 ms           |

---

### Timing Calculated for McEliece-460896

| **Kyber Spec**                | **Complete**       | **Keygen**         | **Enc.**           | **Dec.**           |
|-------------------------------|--------------------|--------------------|--------------------|--------------------|
| Kyber512 + mceliece460896     | 3.66 ms           | 1.74 ms           | 1.75 ms           | 0.16 ms           |
| Kyber768 + mceliece460896     | 3.69 ms           | 1.75 ms           | 1.77 ms           | 0.16 ms           |
| Kyber1024 + mceliece460896    | 3.62 ms           | 1.72 ms           | 1.74 ms           | 0.16 ms           |
| Kyber512 + M.mceliece460896   | 5.95 ms           | 4.03 ms           | 1.75 ms           | 0.16 ms           |
| Kyber768 + M.mceliece460896   | 8.33 ms           | 6.40 ms           | 1.77 ms           | 0.16 ms           |
| Kyber1024 + M.mceliece460896  | 11.14 ms          | 9.24 ms           | 1.74 ms           | 0.16 ms           |

---

### Comparison of Malicious KeyGen Timings

| **Kyber Spec**                | **KeyGen**         | **Malicious KeyGen** | **Difference**     |
|-------------------------------|--------------------|----------------------|--------------------|
| Kyber512 + mceliece348864     | 0.83 ms           | 4.16 ms             | 5.03x             |
| Kyber768 + mceliece348864     | 0.85 ms           | 5.56 ms             | 6.56x             |
| Kyber1024 + mceliece348864    | 0.86 ms           | 7.26 ms             | 8.47x             |
| Kyber512 + mceliece460896     | **1.74 ms**       | 4.03 ms             | 2.32x             |
| Kyber768 + mceliece460896     | **1.75 ms**       | 6.40 ms             | 3.67x             |
| Kyber1024 + mceliece460896    | 1.72 ms           | 9.24 ms             | 5.37x             |

---

### Notes
- All timings are provided in milliseconds (ms).
- Malicious KeyGen timings are calculated based on the modified attack implementation.
- The difference column represents the ratio of the malicious KeyGen time to the standard KeyGen time.



