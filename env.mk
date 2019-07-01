OPT=-O3 -fno-strict-aliasing -fwrapv

ARCH=$(shell gcc -print-multiarch)

ifeq ($(ARCH), arm-linux-gnueabihf)

# For on-board compiling (32-bit ARM)
GPP=g++ -mfp16-format=ieee -march=native -mtune=native
GCC=gcc -mfp16-format=ieee -march=native -mtune=native

else

ifeq ($(ARCH), aarch64-linux-gnu)

# For on-board compiling (64-bit ARM)
GPP=g++ -march=native -mtune=native
GCC=gcc -march=native -mtune=native

else

GPP=g++ -march=native -mtune=native
GCC=gcc -march=native -mtune=native

endif

endif
