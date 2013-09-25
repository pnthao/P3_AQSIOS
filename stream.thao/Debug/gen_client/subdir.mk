################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
O_SRCS += \
../gen_client/file_source.o \
../gen_client/gen_output.o \
../gen_client/generic_client.o \
../gen_client/script_file_reader.o 

CC_SRCS += \
../gen_client/file_source.cc \
../gen_client/gen_output.cc \
../gen_client/generic_client.cc \
../gen_client/script_file_reader.cc 

OBJS += \
./gen_client/file_source.o \
./gen_client/gen_output.o \
./gen_client/generic_client.o \
./gen_client/script_file_reader.o 

CC_DEPS += \
./gen_client/file_source.d \
./gen_client/gen_output.d \
./gen_client/generic_client.d \
./gen_client/script_file_reader.d 


# Each subdirectory must supply rules for building sources it contributes
gen_client/%.o: ../gen_client/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -D_DM_ -D_LOAD_MANAGE_ -D_CTRL_LOAD_MANAGE_ -D_SYS_STR_ -D_MONITOR_ -I"/home/thao/workspace/p3-aqsios/stream.thao/dsms/include" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o"$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


