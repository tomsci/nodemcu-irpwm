idf_component_register(
 SRCS "irpwm.c"
 INCLUDE_DIRS "." "${CMAKE_CURRENT_BINARY_DIR}" "../nodemcu-turcutils"
 PRIV_REQUIRES
   "base_nodemcu"
   "lua"
   "platform"
)

extmod_declare_module(IRPWM)
