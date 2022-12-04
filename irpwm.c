#include "module.h"
#include "lauxlib.h"
#include <string.h>

#define USE_RMT

#ifdef USE_RMT
#include <driver/rmt.h>
#endif

#ifdef CONFIG_PM_ENABLE
#include <esp_pm.h>
#endif
#include <soc/rtc.h>
#include <esp_log.h>
#include <esp_timer.h>
#include <driver/gpio.h>
#include "task/task.h"
#include "../nodemcu-turcutils/turcutils.h"

#define MAX_PULSES 252
#define IDLE_TIME 6000 // microseconds

// We set CLK_DIV to 80 to give us a tick of 1 microsecond (80MHz / 80 = 1us)
// meaning no translation of times to ticks is required. There are however still
// conversion macros in case we need to revisit that again in the future.
#define CLK_DIV (80)
#define US_TO_TICKS(val) (val)
#define TICKS_TO_US(val) (val)

#define DBG(...) ESP_LOGW("irpwm", __VA_ARGS__)

// Max amount of time (in microseconds) which we can record in pulses buffers
#define PULSE_MAX TICKS_TO_US(INT16_MAX)

typedef struct irpwm_bank {
  int64_t start_time;
  int32_t first_pulse; // Use a int32_t to handle a long gap since the last pulse sequence
  int32_t last_pulse; // Ditto
  uint16_t overflows;
  int16_t buf[MAX_PULSES]; // buf store ticks
  uint8_t count; // in pulses
  uint8_t bank_num;
} irpwm_bank;

// Use all the fields of irpwm_bank that otherwise aren't important for
// transmission (start_time thru to buf inclusive).
#define BANK_TO_RMTITEMS(bankptr) ((rmt_item32_t*)(&((bankptr)->start_time)))
#define MAX_RMT_ITEMS (offsetof(irpwm_bank, count) / sizeof(rmt_item32_t))

#define NUM_BANKS 8

typedef struct irpwm_data
{
  uint64_t last_time; // last time the ISR fired
  uint64_t last_rising_time; // last time the GPIO became high (ie when the IR channel potentially went idle)
  irpwm_bank *current; // points to one of banks
  irpwm_bank banks[NUM_BANKS];
  uint8_t gpio;
  uint8_t channel; // rmt_channel_t
  uint8_t async_sending;
  int timer_ref;
  int callback_ref;
#ifdef CONFIG_PM_ENABLE
  esp_pm_lock_handle_t pm_lock;
#endif
} irpwm_data;

static task_handle_t tx_end_task = 0;
static task_handle_t flush_buffer_task = 0;

static void push_data_table(lua_State *L)
{
  lua_pushlightuserdata(L, &push_data_table);
  lua_rawget(L, LUA_REGISTRYINDEX);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    lua_newtable(L);
    lua_pushlightuserdata(L, &push_data_table);
    lua_pushvalue(L, -2); // dup table
    lua_rawset(L, LUA_REGISTRYINDEX);
    // table is now on top of stack
  }
}

// Allocates irpwm_data if it doesn't already exist
static irpwm_data* dataForPin(lua_State* L, int gpio)
{
  push_data_table(L);
  lua_rawgeti(L, -1, gpio);
  if (lua_isnil(L, -1)) {
    lua_pop(L, 1);
    irpwm_data *data = (irpwm_data *)lua_newuserdata(L, sizeof(irpwm_data));
    memset(data, 0, sizeof(irpwm_data));
    data->gpio = gpio;
    data->timer_ref = LUA_REFNIL;
    data->callback_ref = LUA_REFNIL;
    data->current = data->banks;
    for (uint8_t i = 0; i < NUM_BANKS; i++) {
      data->banks[i].bank_num = i;
    }
    lua_rawseti(L, -2, gpio); // pops data
    lua_pop(L, 1); // indextable
    return data;
  } else {
    void *result = lua_touserdata(L, -1);
    lua_pop(L, 2); // result, indextable
    return (irpwm_data *)result;
  }
}

// Returns NULL if not found
static irpwm_data* dataForChannel(lua_State* L, int channel)
{
  push_data_table(L);
  lua_pushnil(L);
  while (lua_next(L, -2)) {
    irpwm_data *data = (irpwm_data *)lua_touserdata(L, -1);
    lua_pop(L, 1); // Removes value
    if (data->channel == channel) {
      lua_pop(L, 2); // Removes key and table
      return data;
    }
    // Otherwise keep iterating
  }
  // If we get here, not found
  lua_pop(L, 1); // data table
  return NULL;
}

static void freeData(lua_State* L, irpwm_data *data)
{
#ifdef CONFIG_PM_ENABLE
  if (data->pm_lock) {
    esp_pm_lock_delete(data->pm_lock);
    data->pm_lock = 0;
  }
#endif
  push_data_table(L);
  lua_pushnil(L);
  lua_rawseti(L, -2, data->gpio);
  lua_pop(L, 1); // table
}

static irpwm_bank *irpwm_bank_switch(irpwm_data *data)
{
  irpwm_bank *prev = data->current;
  irpwm_bank *next = &data->banks[(prev->bank_num == NUM_BANKS - 1) ? 0 : prev->bank_num + 1];
  next->start_time = 0;
  next->first_pulse = 0;
  next->last_pulse = 0;
  if (next->count != 0) {
    // Buffer has not yet finished being flushed, oh dear
    // Can't log here as we might be called from an ISR
    next->overflows++;
  }
  next->count = 0;
  // overflows is only cleared by irpwm_flush_buffer()
  data->current = next;
  return prev;
}

static void irpwm_isr(void *ctx)
{
  irpwm_data *data = (irpwm_data *)ctx;
  int64_t t = esp_timer_get_time();
  if (data->last_time != 0) {
    int64_t delta = t - data->last_time;
    irpwm_bank* current = data->current;
    const bool bank_empty = current->count == 0 && current->first_pulse == 0;
    if (delta > IDLE_TIME && !bank_empty && data->last_rising_time) {
      // We're starting a sequence after a sufficiently long idle period,
      // so switch buffers for the new sequence and post a task to flush the
      // ex-current buffer
      data->current->last_pulse = US_TO_TICKS(-delta); // Given last_rising_time is set we know this should be negative
      irpwm_bank_switch(data);
      task_post_high(flush_buffer_task, (task_param_t)current);
      data->last_time = t;
      return;
    }
    if (bank_empty) {
      current->start_time = data->last_time;
    }

    int16_t sign;
    if (gpio_get_level(data->gpio) == 0) {
      // Falling edge
      sign = -1;
      data->last_rising_time = 0;
    } else {
      sign = 1;
      data->last_rising_time = t;
    }

    if (bank_empty) {
      current->first_pulse = sign * delta;
    } else {
      const int16_t max_val = US_TO_TICKS(sign * PULSE_MAX);
      int16_t* startp = current->buf + current->count;
      int16_t* p = startp;
      int16_t* endp = current->buf + MAX_PULSES;
      while (delta >= PULSE_MAX) {
        if (p == endp) {
          current->overflows++;
        } else {
          *p++ = max_val;
        }
        delta -= PULSE_MAX;
      }
      if (delta) {
        if (p == endp) {
          current->overflows++;
        } else {
          *p++ = (int16_t)US_TO_TICKS(sign * delta);
        }
      }
      current->count += (p - startp);
    }
  }
  data->last_time = t;
}

static void check_err(lua_State *L, esp_err_t err)
{
  switch (err)
  {
    case ESP_ERR_INVALID_ARG: luaL_error(L, "invalid argument");
    case ESP_ERR_INVALID_STATE: luaL_error(L, "internal logic error");
    case ESP_OK: break;
    default: luaL_error(L, "ESP Error %d!", err);
  }
}

#define samesign16(x, y) (((x) & 0x8000) == ((y) & 0x8000))

static void irpwm_flush_buffer(irpwm_bank *bank)
{
  irpwm_data* data = (irpwm_data*)((uintptr_t)bank - sizeof(irpwm_bank) * bank->bank_num - offsetof(irpwm_data, banks));

  int count = bank->count;
  int table_count = count;
  int first_pulse = bank->first_pulse;
  if (first_pulse) table_count++;
  int last_pulse = bank->last_pulse;
  if (last_pulse) table_count++;
  int16_t* buf = bank->buf;

  lua_State *L = lua_getstate();

  uint64_t t = esp_timer_get_time();
  lua_rawgeti(L, LUA_REGISTRYINDEX, data->callback_ref);
  lua_createtable(L, table_count, 1);
  int table_idx = 1;
  if (first_pulse) {
    lua_pushinteger(L, TICKS_TO_US(first_pulse));
    lua_rawseti(L, -2, table_idx++);
  }

  for (int i = 0; i < count; i++) {
    int16_t buf_i = buf[i];
    int val = (int)buf_i;
    // Combine any successive values of the same sign into one
    // (We store them like that because int16_t only lets us store up to 32ms)
    while (i+1 < count && samesign16(buf_i, buf[i+1])) {
      val += buf[++i];
    }
    lua_pushinteger(L, TICKS_TO_US(val));
    lua_rawseti(L, -2, table_idx++);
  }

  if (last_pulse) {
    lua_pushinteger(L, TICKS_TO_US(last_pulse));
    lua_rawseti(L, -2, table_idx++);
  }

  turcutils_pushint64(L, bank->start_time);
  lua_setfield(L, -2, "starttime");

  turcutils_pushint64(L, t);
  lua_setfield(L, -2, "flushtime");

  lua_pushinteger(L, bank->overflows);
  lua_setfield(L, -2, "overflows");

  lua_pushinteger(L, bank->bank_num);
  lua_setfield(L, -2, "bank_num");

  bank->count = 0;
  bank->overflows = 0;

  t = esp_timer_get_time() - t;
  lua_pushinteger(L, (lua_Integer)t);
  lua_call(L, 2, 0); // callback(data, flush_buffer_time)
}

static void irpwm_flush_buffer_task(task_param_t param, task_prio_t prio)
{
  // DBG("Flushing buffer %d", ((irpwm_bank*)param)->bank_num);
  irpwm_flush_buffer((irpwm_bank*)param);
}

static int timer_tick(lua_State *L)
{
  const void *ptr = lua_touserdata(L, lua_upvalueindex(1));
  int64_t t = esp_timer_get_time();
  irpwm_data *data = (irpwm_data *)ptr;
  check_err(L, gpio_intr_disable(data->gpio));
  if (data->current->count && t > data->last_time + IDLE_TIME) {
    data->current->last_pulse = US_TO_TICKS(-(t - data->last_time));
    data->last_time = t;
    irpwm_bank* prev = irpwm_bank_switch(data);
    check_err(L, gpio_intr_enable(data->gpio));

    // Don't call irpwm_flush_buffer directly here (even though we could)
    // because then we risk being out of order wrt the ISR-posted flushes
    // because timers hang off a completely different priority interrupt queue.
    // DBG("Queuing buffer flush of %d from timer", prev->bank_num);
    task_post_high(flush_buffer_task, (task_param_t)prev);
  } else {
    check_err(L, gpio_intr_enable(data->gpio));
  }
  return 0;
}

#ifdef USE_RMT
// static int irpwm_listen_rmt(lua_State *L)
// {
//   int gpio = luaL_checkint(L, 1);
//   lua_settop(L, 2);

//   rmt_config_t config;
//   config.rmt_mode = RMT_MODE_RX;
//   config.channel = RMT_CHANNEL_0;
//   config.clk_div = 80; // ie tick is 80Mhz/80 = 1us which is nice and tidy
//   config.gpio_num = gpio;
//   config.mem_block_num = 1;
//   config.rx_config.filter_en = true;
//   config.rx_config.filter_ticks_thresh = 100;
//   config.rx_config.idle_threshold = 9500;
//   check_err(L, rmt_config(&config));
//   check_err(L, rmt_driver_install(config.channel, 1000, 0));
//   return 0;
// }

static int irpwm_txconfig(lua_State *L)
{
  int gpio = luaL_checkint(L, 1);
  irpwm_data* data = dataForPin(L, gpio);

  if (lua_isnoneornil(L, 2)) {
    freeData(L, data);
    return 0;
  }
  int channel = luaL_checkint(L, 2);
  luaL_argcheck(L, 2, channel >= RMT_CHANNEL_0 && channel < RMT_CHANNEL_MAX, "Bad channel id");


#ifdef CONFIG_PM_ENABLE
  if (!data->pm_lock) {
    check_err(L, esp_pm_lock_create(ESP_PM_APB_FREQ_MAX, 0, "irpwm", &data->pm_lock));
  }
#endif

  rmt_config_t config;
  config.rmt_mode = RMT_MODE_TX;
  config.channel = channel;
  config.clk_div = CLK_DIV;
  config.gpio_num = gpio;
  config.mem_block_num = 1;
  config.tx_config.loop_en = false;
  config.tx_config.carrier_duty_percent = 50;
  config.tx_config.carrier_freq_hz = 38000;
  config.tx_config.carrier_level = 1;
  config.tx_config.carrier_en = true;
  config.tx_config.idle_level = 0;
  config.tx_config.idle_output_en = true;

  check_err(L, rmt_config(&config));
  data->channel = config.channel;
  check_err(L, rmt_driver_install(config.channel, 0, 0));
  return 0;
}

static inline void set_item0(rmt_item32_t *item, int val)
{
  if (val > 0) {
    item->level0 = 1;
    item->duration0 = US_TO_TICKS(val);
  } else {
    item->level0 = 0;
    item->duration0 = US_TO_TICKS(-val);
  }
}

static inline void set_item1(rmt_item32_t *item, int val)
{
  if (val > 0) {
    item->level1 = 1;
    item->duration1 = US_TO_TICKS(val);
  } else {
    item->level1 = 0;
    item->duration1 = US_TO_TICKS(-val);
  }
}

static void push_item(lua_State* L, rmt_item32_t **ptr, const rmt_item32_t* endptr, int val)
{
  if (*ptr == endptr) {
    luaL_error(L, "Exceeded maximum number of rmt_item32_t elements!");
  }

  if ((*ptr)->val) {
    set_item1(*ptr, val);
    *ptr += 1;
    if (*ptr != endptr) {
      (*ptr)->val = 0;
    }
  } else {
    set_item0(*ptr, val);
  }
}

static const int kMaxPulseLength = TICKS_TO_US(32767); // in microseconds

static int populate_items(lua_State *L, int idx, rmt_item32_t *items, int max_items)
{
  rmt_item32_t* item_ptr = items;
  item_ptr->val = 0;
  const rmt_item32_t* endp = items + max_items;
  for (int i = 1; item_ptr != endp; i++) {
    lua_rawgeti(L, idx, i);
    int val = lua_tointeger(L, -1);
    lua_pop(L, 1);
    if (val == 0) {
      break;
    }

    // Handle the case where a pulse is too long to fit in a single 15-bit
    // rmt_item32_t duration by splitting it up
    int sign = val < 0 ? -1 : 1;
    int absval = val * sign;
    while (absval > 0) {
      int chunk = absval < kMaxPulseLength ? absval : kMaxPulseLength;
      push_item(L, &item_ptr, endp, chunk * sign);
      absval -= chunk;
    }
  }

  if (item_ptr != endp && item_ptr->val) {
    item_ptr++;
  }

  return (int)(item_ptr - items);
}

static void irpwm_tx_end_task(task_param_t param, task_prio_t prio)
{
  // DBG("+irpwm_tx_end_task");
  rmt_channel_t channel = (rmt_channel_t)param;
  irpwm_data *data = dataForChannel(lua_getstate(), channel);
  irpwm_bank *repeats = data ? &data->banks[1] : NULL;
  if (repeats && repeats->count) {
    // Indicates there's a repeat to send
    rmt_item32_t *items = BANK_TO_RMTITEMS(repeats);
    esp_err_t err = rmt_write_items(channel, items, repeats->count, false);
    if (err) {
      ESP_LOGW("irpwm", "Error from repeat rmt_write_items: %d", err);
    }
  } else if (data && data->async_sending) {
#ifdef CONFIG_PM_ENABLE
    esp_pm_lock_release(data->pm_lock);
#endif
    data->async_sending = 0;
  }
  // DBG("-irpwm_tx_end_task");
}

static void irpwm_tx_end(rmt_channel_t channel, void * arg)
{
  // This is called in ISR context so post a task to handle completion
  task_post_low(tx_end_task, channel);
}

// irpwm.send(gpio, pulses [, repeats])
// repeats being set implies sending asynchronously
static int irpwm_send(lua_State *L)
{
  int gpio = luaL_checkint(L, 1);
  irpwm_data* data = dataForPin(L, gpio);
  bool async = !lua_isnoneornil(L, 3);

  if (data->async_sending) {
    return luaL_error(L, "Cannot send again before the previous send has completed");
  }

#ifdef CONFIG_PM_ENABLE
  check_err(L, esp_pm_lock_acquire(data->pm_lock));
#endif

  if (async) {
      data->async_sending = 1;
  }

  // Use bank 0 for send pulses, bank 1 for repeats
  rmt_item32_t *items = BANK_TO_RMTITEMS(&data->banks[0]);
  int nitems = populate_items(L, 2, items, MAX_RMT_ITEMS);

  irpwm_bank* repeats = &data->banks[1];
  if (async) {
    rmt_item32_t *reps = BANK_TO_RMTITEMS(repeats);
    repeats->count = populate_items(L, 3, reps, MAX_RMT_ITEMS);
    if (!tx_end_task) {
      tx_end_task = task_get_id(irpwm_tx_end_task);
    }
    rmt_register_tx_end_callback(irpwm_tx_end, NULL);
  } else {
    repeats->count = 0;
  }

  esp_err_t err = rmt_write_items((rmt_channel_t)data->channel, items, nitems, !async);

  if (!async || err) {
#ifdef CONFIG_PM_ENABLE
    esp_pm_lock_release(data->pm_lock);
#endif
    data->async_sending = 0;
  }

  check_err(L, err);
  return 0;
}

// irpwm.stopsend(gpio)
static int irpwm_stopsend(lua_State *L)
{
  // DBG("+stopsend");
  int gpio = luaL_checkint(L, 1);
  irpwm_data* data = dataForPin(L, gpio);
  data->banks[1].count = 0; // Prevents any further repeats
  check_err(L, rmt_wait_tx_done(data->channel, portMAX_DELAY));
  if (data->async_sending) {
    // Ie if the irpwm_tx_end_task hasn't run yet, manually release the lock
    // and reset so we're immediately ready to send again
#ifdef CONFIG_PM_ENABLE
    esp_pm_lock_release(data->pm_lock);
#endif
    data->async_sending = 0;
  }
  // DBG("-stopsend");
  return 0;
}

#endif

// irpwm.listen(gpio, callbackFn)
static int irpwm_listen(lua_State* L)
{
  int gpio = luaL_checkint(L, 1);
  irpwm_data* data = dataForPin(L, gpio);

  if (lua_isnoneornil(L, 2)) {
    if (data->timer_ref != LUA_REFNIL) {
      lua_rawgeti(L, LUA_REGISTRYINDEX, data->timer_ref);
      luaL_callmeta(L, -1, "unregister");
      luaL_unref(L, LUA_REGISTRYINDEX, data->timer_ref);
      data->timer_ref = LUA_REFNIL;
    }
    if (data->callback_ref != LUA_REFNIL) {
      luaL_unref(L, LUA_REGISTRYINDEX, data->callback_ref);
      data->callback_ref = LUA_REFNIL;
    }
    check_err(L, gpio_intr_disable(gpio));
    check_err(L, gpio_isr_handler_remove(gpio));
    return 0;
  }

  lua_settop(L, 2);

  check_err(L, gpio_intr_disable(gpio));

  data->gpio = gpio;
  irpwm_bank_switch(data);
  data->last_time = 0;

  check_err(L, gpio_set_intr_type(gpio, GPIO_INTR_ANYEDGE));
  check_err(L, gpio_isr_handler_add(gpio, irpwm_isr, (void *)data));
  check_err(L, gpio_intr_enable(gpio));

  if (!flush_buffer_task) {
    flush_buffer_task = task_get_id(irpwm_flush_buffer_task);
  }

  lua_getglobal(L, "tmr"); // stack position 3
  lua_getfield(L, -1, "create");
  lua_call(L, 0, 1);

  // timer obj now at top
  lua_getfield(L, -1, "alarm");
  lua_pushvalue(L, -2); // timer obj
  lua_pushinteger(L, 20); // interval_ms. Ugh, 10ms is the minimum granularity for tmr due to it being the freertos tick rate
  lua_getfield(L, 3, "ALARM_AUTO"); // mode

  lua_pushlightuserdata(L, data);
  lua_pushcclosure(L, timer_tick, 1);

  lua_call(L, 4, 1);
  if (!lua_toboolean(L, -1)) {
    return luaL_error(L, "irpwm: Failed to create timer!");
  }
  lua_pop(L, 1); // timer obj back on top
  data->timer_ref = luaL_ref(L, LUA_REGISTRYINDEX);
  lua_pop(L, 1); // tmr
  data->callback_ref = luaL_ref(L, LUA_REGISTRYINDEX);

  return 0;
}

LROT_BEGIN(irpwm, NULL, 0)
  LROT_FUNCENTRY(listen, irpwm_listen)
  LROT_FUNCENTRY(txconfig, irpwm_txconfig)
  LROT_FUNCENTRY(send, irpwm_send)
  LROT_FUNCENTRY(stopsend, irpwm_stopsend)
LROT_END(irpwm, NULL, 0)

NODEMCU_MODULE(IRPWM, "irpwm", irpwm, NULL);
