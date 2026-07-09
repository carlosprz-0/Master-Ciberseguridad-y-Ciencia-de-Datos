#include "crypto_aead.h"

#include <stdint.h>
#include <string.h>

#include "api.h"
#include "ascon.h"
#include "constants.h"
#include "permutations.h"
#include "word.h"

static void ascon_initialize(
    ascon_state_t* s,
    const unsigned char* k,
    const unsigned char* npub
) {
  uint64_t K0 = LOADBYTES(k, 8);
  uint64_t K1 = LOADBYTES(k + 8, 8);
  uint64_t N0 = LOADBYTES(npub, 8);
  uint64_t N1 = LOADBYTES(npub + 8, 8);

  s->x[0] = ASCON_128A_IV;
  s->x[1] = K0;
  s->x[2] = K1;
  s->x[3] = N0;
  s->x[4] = N1;

  P12(s);

  s->x[3] ^= K0;
  s->x[4] ^= K1;
}

static void ascon_process_associated_data(
    ascon_state_t* s,
    const unsigned char* ad,
    unsigned long long adlen
) {
  if (adlen > 0) {
    while (adlen >= ASCON_AEAD_RATE) {
      s->x[0] ^= LOADBYTES(ad, 8);
      s->x[1] ^= LOADBYTES(ad + 8, 8);

      P8(s);

      ad += ASCON_AEAD_RATE;
      adlen -= ASCON_AEAD_RATE;
    }

    if (adlen >= 8) {
      s->x[0] ^= LOADBYTES(ad, 8);
      s->x[1] ^= LOADBYTES(ad + 8, adlen - 8);
      s->x[1] ^= PAD(adlen - 8);
    } else {
      s->x[0] ^= LOADBYTES(ad, adlen);
      s->x[0] ^= PAD(adlen);
    }

    P8(s);
  }

  s->x[4] ^= DSEP();
}

static void ascon_finalize(
    ascon_state_t* s,
    const unsigned char* k,
    unsigned char* tag
) {
  uint64_t K0 = LOADBYTES(k, 8);
  uint64_t K1 = LOADBYTES(k + 8, 8);

  s->x[2] ^= K0;
  s->x[3] ^= K1;

  P12(s);

  s->x[3] ^= K0;
  s->x[4] ^= K1;

  STOREBYTES(tag, s->x[3], 8);
  STOREBYTES(tag + 8, s->x[4], 8);
}

static int ascon_verify_tag(
    const unsigned char* tag1,
    const unsigned char* tag2
) {
  unsigned char diff = 0;

  for (int i = 0; i < CRYPTO_ABYTES; i++) {
    diff |= tag1[i] ^ tag2[i];
  }

  return diff == 0 ? 0 : -1;
}

int crypto_aead_encrypt(
    unsigned char* c,
    unsigned long long* clen,
    const unsigned char* m,
    unsigned long long mlen,
    const unsigned char* ad,
    unsigned long long adlen,
    const unsigned char* nsec,
    const unsigned char* npub,
    const unsigned char* k
) {
  (void)nsec;

  ascon_state_t s;
  ascon_initialize(&s, k, npub);
  ascon_process_associated_data(&s, ad, adlen);

  unsigned char* ciphertext_start = c;
  unsigned long long remaining = mlen;

  while (remaining >= ASCON_AEAD_RATE) {
    s.x[0] ^= LOADBYTES(m, 8);
    s.x[1] ^= LOADBYTES(m + 8, 8);

    STOREBYTES(c, s.x[0], 8);
    STOREBYTES(c + 8, s.x[1], 8);

    P8(&s);

    m += ASCON_AEAD_RATE;
    c += ASCON_AEAD_RATE;
    remaining -= ASCON_AEAD_RATE;
  }

  if (remaining >= 8) {
    s.x[0] ^= LOADBYTES(m, 8);
    s.x[1] ^= LOADBYTES(m + 8, remaining - 8);

    STOREBYTES(c, s.x[0], 8);
    STOREBYTES(c + 8, s.x[1], remaining - 8);

    s.x[1] ^= PAD(remaining - 8);
  } else {
    s.x[0] ^= LOADBYTES(m, remaining);

    STOREBYTES(c, s.x[0], remaining);

    s.x[0] ^= PAD(remaining);
  }

  c += remaining;

  ascon_finalize(&s, k, c);

  *clen = mlen + CRYPTO_ABYTES;

  return 0;
}

int crypto_aead_decrypt(
    unsigned char* m,
    unsigned long long* mlen,
    unsigned char* nsec,
    const unsigned char* c,
    unsigned long long clen,
    const unsigned char* ad,
    unsigned long long adlen,
    const unsigned char* npub,
    const unsigned char* k
) {
  (void)nsec;

  if (clen < CRYPTO_ABYTES) {
    return -1;
  }

  unsigned long long ciphertext_len = clen - CRYPTO_ABYTES;
  const unsigned char* tag = c + ciphertext_len;

  ascon_state_t s;
  ascon_initialize(&s, k, npub);
  ascon_process_associated_data(&s, ad, adlen);

  unsigned char* plaintext_start = m;
  unsigned long long remaining = ciphertext_len;

  while (remaining >= ASCON_AEAD_RATE) {
    uint64_t C0 = LOADBYTES(c, 8);
    uint64_t C1 = LOADBYTES(c + 8, 8);

    STOREBYTES(m, s.x[0] ^ C0, 8);
    STOREBYTES(m + 8, s.x[1] ^ C1, 8);

    s.x[0] = C0;
    s.x[1] = C1;

    P8(&s);

    c += ASCON_AEAD_RATE;
    m += ASCON_AEAD_RATE;
    remaining -= ASCON_AEAD_RATE;
  }

  if (remaining >= 8) {
    uint64_t C0 = LOADBYTES(c, 8);
    uint64_t C1 = LOADBYTES(c + 8, remaining - 8);

    STOREBYTES(m, s.x[0] ^ C0, 8);
    STOREBYTES(m + 8, s.x[1] ^ C1, remaining - 8);

    s.x[0] = C0;
    s.x[1] = CLEARBYTES(s.x[1], remaining - 8) ^ C1;
    s.x[1] ^= PAD(remaining - 8);
  } else {
    uint64_t C0 = LOADBYTES(c, remaining);

    STOREBYTES(m, s.x[0] ^ C0, remaining);

    s.x[0] = CLEARBYTES(s.x[0], remaining) ^ C0;
    s.x[0] ^= PAD(remaining);
  }

  unsigned char expected_tag[CRYPTO_ABYTES];
  ascon_finalize(&s, k, expected_tag);

  if (ascon_verify_tag(expected_tag, tag) != 0) {
    memset(plaintext_start, 0, ciphertext_len);
    return -1;
  }

  *mlen = ciphertext_len;

  return 0;
}