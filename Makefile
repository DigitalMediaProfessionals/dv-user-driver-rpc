include ./env.mk

.PHONY: all clean install install_lib install_server

all:	libdmpdv_rpc.so dmpdv_server

weights_conv.o:	dv-user-driver/src/weights_conv.c
	$(GCC) -c -fPIC dv-user-driver/src/weights_conv.c -o weights_conv.o -std=c99 -Wall -Wno-unused-function -Werror -I./dv-user-driver/include $(OPT)

weights_dil.o:	dv-user-driver/src/weights_dil.c
	$(GCC) -c -fPIC dv-user-driver/src/weights_dil.c -o weights_dil.o -std=c99 -Wall -Wno-unused-function -Werror -I./dv-user-driver/include $(OPT)

weights_fc.o:	dv-user-driver/src/weights_fc.c
	$(GCC) -c -fPIC dv-user-driver/src/weights_fc.c -o weights_fc.o -std=c99 -Wall -Wno-unused-function -Werror -I./dv-user-driver/include $(OPT)

libdmpdv_rpc.so:	src/dmpdv_rpc.cpp weights_conv.o weights_dil.o weights_fc.o
	$(GPP) -c -fPIC src/dmpdv_rpc.cpp -o dmpdv_rpc.o -std=c++11 -Wall -Werror -I./include -I./dv-user-driver/include $(OPT)
	$(GCC) -shared -fPIC dmpdv_rpc.o weights_conv.o weights_dil.o weights_fc.o -o libdmpdv_rpc.so $(OPT) -lstdc++

dmpdv_server:	src/dmpdv_server.cpp
	$(MAKE) -C dv-user-driver libdmpdv.so
	$(GPP) src/dmpdv_server.cpp -o dmpdv_server -std=c++11 -Wall -Werror -I./include -I./dv-user-driver/include $(OPT) -L./dv-user-driver -ldmpdv

clean:
	rm -f *.o libdmpdv_rpc.so dmpdv_server
	$(MAKE) -C dv-user-driver clean

.SILENT:	install install_lib install_server

install:
	echo "Use 'make install_server' or 'make install_lib'"

install_lib:	libdmpdv_rpc.so
	echo "Copying libdmpdv_rpc.so to /usr/local/lib/"
	cp libdmpdv_rpc.so /usr/local/lib/
	echo "Creating link /usr/local/lib/libdmpdv.so => /usr/local/lib/libdmpdv_rpc.so"
	rm -f /usr/local/lib/libdmpdv.so
	ln -s /usr/local/lib/libdmpdv_rpc.so /usr/local/lib/libdmpdv.so
	echo "ldconfig"
	ldconfig
	echo "Copying header files"
	cp dv-user-driver/include/dmp_dv*.h /usr/include/
	echo "Done"

install_server:	dmpdv_server
	echo "Copying dmpdv_server to /usr/local/bin/"
	cp dmpdv_server /usr/local/bin/
	echo "Done"
