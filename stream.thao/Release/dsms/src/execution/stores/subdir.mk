################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../dsms/src/execution/stores/lin_store_impl.o \
../dsms/src/execution/stores/pwin_store_impl.o \
../dsms/src/execution/stores/rel_store_impl.o \
../dsms/src/execution/stores/simple_store.o \
../dsms/src/execution/stores/win_store_impl.o 

CC_SRCS += \
../dsms/src/execution/stores/lin_store_impl.cc \
../dsms/src/execution/stores/pwin_store_impl.cc \
../dsms/src/execution/stores/rel_store_impl.cc \
../dsms/src/execution/stores/simple_store.cc \
../dsms/src/execution/stores/win_store_impl.cc 

OBJS += \
./dsms/src/execution/stores/lin_store_impl.o \
./dsms/src/execution/stores/pwin_store_impl.o \
./dsms/src/execution/stores/rel_store_impl.o \
./dsms/src/execution/stores/simple_store.o \
./dsms/src/execution/stores/win_store_impl.o 

CC_DEPS += \
./dsms/src/execution/stores/lin_store_impl.d \
./dsms/src/execution/stores/pwin_store_impl.d \
./dsms/src/execution/stores/rel_store_impl.d \
./dsms/src/execution/stores/simple_store.d \
./dsms/src/execution/stores/win_store_impl.d 


# Each subdirectory must supply rules for building sources it contributes
dsms/src/execution/stores/%.o: ../dsms/src/execution/stores/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D_DM_ -D_CTRL_LOAD_MANAGE_ -D_LOAD_MANAGE_ -D_SYS_STR_ -D_MONITOR_ -I"/home/thao/workspace/stream.thao/dsms/include" -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


