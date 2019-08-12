#include <Wire.h>
#include "SdFat.h"
#include <RTClib.h>
#include <Adafruit_ADS1015_NB.h>
#include <SPI.h>

SdFat SD;
#define SD_CS_PIN 10

RTC_PCF8523 RTC;

Adafruit_ADS1115 ADCS[4] = {Adafruit_ADS1115(0x48),
                            Adafruit_ADS1115(0x49),
                            Adafruit_ADS1115(0x4A),
                            Adafruit_ADS1115(0x4B)};

// OPC Measurement.
uint8_t OPC_UPDATE_RATE_SECS = 30;
int _CS = 9;
//int PIN_OPC_POWER = A1;
char SPI_in[86];

// ADC Measurement.
uint16_t ADC_UPDATE_RATE_SECS = 10;
const uint8_t ADC_COUNT = 15;
int32_t ADC_TOTALS[ADC_COUNT]={0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
const uint8_t ADCS_PER_CHANNEL[4] = {4, 4, 4, 3};
unsigned long ADC_TIMESTAMP = 0;
const unsigned long ADC_INTEGRATION_TIME_MSECS = 126;
uint16_t ITERATION_INDEX = 0;

// Battery voltage measurement.
const int PIN_BATT_VPLUS = A1;
uint8_t BATT_UPDATE_RATE_SECS = 30;
uint16_t BATT_ITERATION_INDEX = 0;
uint32_t BATT_TOTAL = 0;
unsigned long BATT_TIMESTAMP = 0;

// Inbound message handling.
const int NUM_COMMANDS = 3;
const int MAX_COMMAND_LENGTH = 2;
const char COMMAND_STRINGS[NUM_COMMANDS][MAX_COMMAND_LENGTH] = {"ST", "TX", "CP"};
const byte COMMAND_LENGTHS[NUM_COMMANDS] = {2, 2, 2};
const int MAX_INPUT = 9;
byte INPUT_MODE = 0;    // 0 = message, 1 = CRC8 check.
char INPUT_MESSAGE_BUFFER[MAX_INPUT];
char INPUT_CRC_BUFFER[4];
byte INPUT_MESSAGE_LENGTH = 0;
byte INPUT_CRC_LENGTH = 0;
int RECEIVE_INDEX = 0;
bool INCOMING_STARTED = false;
int COMMAND_MODE = 0;
int COMMAND_STAGE = 0;
unsigned long COMMAND_START_TIMESTAMP = 0;
int TIMESTAMP_BUFFER[6];
const char TIME_HEADER_STRINGS[6][2] = {"YY", "MM", "DD", "hh", "mm", "ss"};
byte TIME_COMMAND_MODE = 0;     // 0 = Header, 1 = Value.
bool CRC_SUCCESS = false;
const int NUM_PARAM_STRINGS = 3;
const char PARAM_HEADER_STRINGS[NUM_PARAM_STRINGS][2] = {"AP", "OP", "UR"};
byte PARAM_COMMAND_MODE = 0;

// Outbound message handling.
byte SPOOL_COMMAND_MODE = 0;
const int NUM_SPOOL_COMMANDS = 3;
const int MAX_SPOOL_COMMAND_LENGTH = 2;
const char SPOOL_COMMAND_STRINGS[NUM_SPOOL_COMMANDS][MAX_SPOOL_COMMAND_LENGTH] = {"AK", "AR", "CC"};
const byte SPOOL_COMMAND_LENGTHS[NUM_SPOOL_COMMANDS] = {2, 2, 2};
const byte OUTPUT_PACKET_LENGTH = 128;
char OUTPUT_BUFFER[OUTPUT_PACKET_LENGTH + 1];
char FILENAME_BUFFER[13];
unsigned long SEND_FILE_CURSOR = 0;
unsigned long DATA_FILE_CURSOR = 0;
bool EOF_FLAG = false;

// RPI control.
uint8_t RPI_UPDATE_RATE_MINS = 2;
unsigned long RPI_MODE_TIMESTAMP = 0;
byte RPI_MODE = 0;    // 0 = Off, 1 = Booting up, 2 = Running, 3 = Shutting down.
const int PIN_RPI_RUNNING = 2;
const int PIN_RPI_POWER = 8;
const int PIN_RPI_MODE_0 = 6;
const int PIN_RPI_MODE_1 = 5;
const int PIN_RPI_MODE_2 = 4;
const int PIN_RPI_MODE_3 = 3;

//// Misc pins
//const int PIN_RTC_FAIL_WARN = 6;
//const int PIN_SD_FAIL_WARN = 7;

void setup() {
  // Setup pins.
  digitalWrite(_CS, HIGH);
  pinMode(_CS, OUTPUT);
  
  digitalWrite(PIN_RPI_POWER, HIGH);
  pinMode(PIN_RPI_POWER, OUTPUT);

  digitalWrite(PIN_RPI_MODE_0, HIGH);
  pinMode(PIN_RPI_MODE_0, OUTPUT);
  digitalWrite(PIN_RPI_MODE_1, LOW);
  pinMode(PIN_RPI_MODE_1, OUTPUT);
  digitalWrite(PIN_RPI_MODE_2, LOW);
  pinMode(PIN_RPI_MODE_2, OUTPUT);
  digitalWrite(PIN_RPI_MODE_3, LOW);
  pinMode(PIN_RPI_MODE_3, OUTPUT);
  
//  digitalWrite(PIN_OPC_POWER, LOW);
//  pinMode(PIN_OPC_POWER, OUTPUT);
  
  //digitalWrite(PIN_RTC_FAIL_WARN, LOW);
  //digitalWrite(PIN_SD_FAIL_WARN, LOW);
  
//  pinMode(PIN_RTC_FAIL_WARN, OUTPUT);
//  pinMode(PIN_SD_FAIL_WARN, OUTPUT);
  
  digitalWrite(PIN_RPI_RUNNING, LOW);
  pinMode(PIN_RPI_RUNNING, INPUT);
  
  pinMode(PIN_BATT_VPLUS, INPUT);
  
  Serial.begin(115200L);
  delay(500);
  
  // Initialise Real Time Clock.
  if (! RTC.begin()) {
    //digitalWrite(PIN_RTC_FAIL_WARN, HIGH);
    while (1);
  }
  
  // INITIALISE SD CARD:
  if (! SD.begin(SD_CS_PIN)) {
    //digitalWrite(PIN_SD_FAIL_WARN, HIGH);
    while (true) {}
  }
  
  // Initialise ADCs.
  initialiseChannel(0);
  initialiseChannel(1);
  initialiseChannel(2);
  initialiseChannel(3);
  
  // Set I2C clock frequency above the default.
  Wire.setClock(400000L);
  
  // Start-up the OPC.
  bool fan_state = OPCN3_setOPCState(0, true);
  delay(5000);
  bool laser_state = OPCN3_setOPCState(1, true);
  delay(1000);
  if (! (fan_state && laser_state)) {
    while (true) {}
  }
  
  analogReference(EXTERNAL);
  
  RPI_MODE_TIMESTAMP = millis();
  BATT_TIMESTAMP = millis();
}

void serviceRPI() {
  unsigned long current_timestamp = millis();
  if (RPI_MODE == 0) {
    if (current_timestamp >= (RPI_MODE_TIMESTAMP + (((unsigned long)RPI_UPDATE_RATE_MINS) * 60000UL))) {
      RPI_MODE = 1;
      digitalWrite(PIN_RPI_POWER, LOW);
      digitalWrite(PIN_RPI_MODE_0, LOW);
      digitalWrite(PIN_RPI_MODE_1, HIGH);
      logTelemetry(F("RPI powering up"));
    }
  } else if (RPI_MODE == 1) {
      if (digitalRead(PIN_RPI_RUNNING) == HIGH) {
        RPI_MODE = 2;
        digitalWrite(PIN_RPI_MODE_1, LOW);
        digitalWrite(PIN_RPI_MODE_2, HIGH);
        logTelemetry(F("RPI running"));
      }
  } else if (RPI_MODE == 2) {
    if (digitalRead(PIN_RPI_RUNNING) == LOW) {
      RPI_MODE = 3;
      digitalWrite(PIN_RPI_MODE_2, LOW);
      digitalWrite(PIN_RPI_MODE_3, HIGH);
      logTelemetry(F("RPI powering down"));
      RPI_MODE_TIMESTAMP = millis();
    }
  } else if (RPI_MODE == 3) {
    if (current_timestamp >= (RPI_MODE_TIMESTAMP + 30000L)) {
      digitalWrite(PIN_RPI_POWER, HIGH);
      RPI_MODE = 0;
      digitalWrite(PIN_RPI_MODE_3, LOW);
      digitalWrite(PIN_RPI_MODE_0, HIGH);
      RPI_MODE_TIMESTAMP = millis();
      logTelemetry(F("RPI powered down"));
    }
  }
}

int getLogIndex() {
  // Determine next file number.
  int log_index = 0;
  DateTime filenow;
  // Get the current time from the RTC.
  filenow = RTC.now();
  bool finished = false;
  char filename[13];
  while (true) {
    // Generate current logging filename.
    sprintf(filename, "%04d%02d%02d.%03d", filenow.year(), filenow.month(), filenow.day(), log_index);
    bool response = SD.exists(filename);
    if (response == false) {
      break;
    } else {
      log_index ++;
    }
    delay(20);
  }
  if (log_index > 0) {
    log_index = log_index - 1;
    logTelemetry(F("Arduino reset"));
  }
  return log_index;
}

bool logTelemetry(const char * characters) {
  bool success = false;
  DateTime lognow;
  File LOG_FILE;
  // Get the current time from the RTC.
  lognow = RTC.now();
  char string_buffer[11];
  strcpy_PF(string_buffer, F("LOG.CSV"));
  LOG_FILE = SD.open(string_buffer, O_CREAT | O_WRITE | O_APPEND);
  int n = 0;
  for (const char * i = characters; *i != '\0'; i ++) {
    n += 1;
  }
  if (LOG_FILE) {
    success = true;
    ultoa(lognow.unixtime(), string_buffer, 10);
    LOG_FILE.write(string_buffer, 10);
    LOG_FILE.write(',');
    LOG_FILE.write(characters, n);
    LOG_FILE.write("\r\n", 2);
    LOG_FILE.close();
  }
  return success;
}

void logTelemetry(const __FlashStringHelper* characters) {
  int len = 0;
  for (const char PROGMEM *ptr = (const char PROGMEM *)characters; pgm_read_byte(ptr) != '\0'; ptr ++) {
    len ++;
  }
  char string_buffer[len + 1];
  memcpy_P(string_buffer, characters, len);
  string_buffer[len] = '\0';
  logTelemetry(string_buffer);
  return 0;
}

bool logTextToSD(DateTime now, int log_index, const char * characters) {
  bool success = false;
  char string_buffer[13];
  File DATA_FILE;
  // Generate current logging filename.
  sprintf(string_buffer, "%04d%02d%02d.%03d", now.year(), now.month(), now.day(), log_index);
  DATA_FILE = SD.open(string_buffer, O_CREAT | O_WRITE | O_APPEND);
  int n = 0;
  for (const char * i = characters; *i != '\0'; i ++) {
    n += 1;
  }
  if (DATA_FILE) {
    success = true;
    DATA_FILE.write(characters, n);
    DATA_FILE.write("\r\n", 2);
    DATA_FILE.close();
  }
  return success;
}

void logTextToSD(DateTime now, int log_index, const __FlashStringHelper* characters) {
  int len = 0;
  for (const char PROGMEM *ptr = (const char PROGMEM *)characters; pgm_read_byte(ptr) != '\0'; ptr ++) {
    len ++;
  }
  char string_buffer[len + 1];
  memcpy_P(string_buffer, characters, len);
  string_buffer[len] = '\0';
  logTextToSD(now, log_index, string_buffer);
  return 0;
}

bool logFileToSend(const char * characters) {
  bool success = false;
  File SEND_FILE;
  char string_buffer[9];
  strcpy_PF(string_buffer, F("SEND.TXT"));
  SEND_FILE = SD.open(string_buffer, O_CREAT | O_WRITE | O_APPEND);
  if (SEND_FILE) {
    success = true;
    for (int n = 0; *characters != '\0'; characters ++) {
      SEND_FILE.print(*characters);
    }
    SEND_FILE.write("\r\n", 2);
    SEND_FILE.close();
  }
  return success;
}

void loop() {
  DateTime now = RTC.now();
  DateTime last_log = now;
  int32_t adc_last_time = now.unixtime();
  int32_t opc_last_time = now.unixtime();
  int32_t batt_last_time = now.unixtime();
  //uint8_t file_change_last_day = now.day();
  int log_index = getLogIndex();
  char filename[13];
  uint8_t adc_mode = 0;
  while (true) {
    // Listen for any new incoming characters over serial connection, set new mode appropriately.
    bool message_to_process;
    int mode;
    byte message_type;
    unsigned long temp_timestamp;
    
    serviceRPI();
    
    if (COMMAND_MODE == 0) {      // Idle, waiting for commands...
      message_to_process = serialListen(false);
      if (message_to_process == true) {
        if (CRC_SUCCESS == true) {
          message_type = 0;
          mode = parseMessage(message_to_process, message_type);          // 0 = No change, 1 = Set time...
          if (mode == 0) {
            serialSpeak(F("fl"));
            COMMAND_MODE = 0;
          } else {
            if (mode == 1) {
              serialSpeak(F("ak"));
              COMMAND_MODE = 1;
              TIME_COMMAND_MODE = 0;
              logTelemetry(F("Time update started"));
            }
            if (mode == 2) {
              sprintf(filename, "%04d%02d%02d.%03d", last_log.year(), last_log.month(), last_log.day(), log_index);   // These lines add the current file to the 'to-send' list
              logFileToSend(filename);                                                                                // Then increase the index so the next data log will create
              log_index ++;                                                                                           // a new logging file (todays date, index incremented).
              logTextToSD(last_log, log_index, F("(BLANK_HEADER)"));                            // We immediately create a blank header to ensure that the file
              COMMAND_MODE = 2;                                                                 // is actually created, in case the date changes between the increment of the log index
              SPOOL_COMMAND_MODE = 0;                                                           // and the next time data is written to the SD card. If we don't do this, when the date
              SEND_FILE_CURSOR = 0;                                                             // changes prior to the next SD card write, the current dat file name will be added to the
              logTelemetry(F("Data transmission started"));                                     // SEND file list, even though it does not yet exist. This will cause problems next update.
            }
            if (mode == 3) {
              serialSpeak(F("ak"));
              COMMAND_MODE = 3;
              PARAM_COMMAND_MODE = 0;
              logTelemetry(F("Parameter change started"));
            }
          }
        } else if (CRC_SUCCESS == false) {
          serialSpeak(F("fl"));
          COMMAND_MODE = 0;
        }
        COMMAND_START_TIMESTAMP = millis();
        for (int j = 0; j < MAX_INPUT; j ++) {
          INPUT_MESSAGE_BUFFER[j] = '\0';
        }
      }
    } else {
      applyCommand(COMMAND_MODE);
    }

    adc_mode = readADCS(adc_mode);
    averageBattVolt();
    
    now = RTC.now();
    if ((now.unixtime() >= (adc_last_time + (int32_t)ADC_UPDATE_RATE_SECS)) || (now.unixtime() >= (opc_last_time + (int32_t)OPC_UPDATE_RATE_SECS)) || (now.unixtime() >= (batt_last_time + (int32_t)BATT_UPDATE_RATE_SECS))) {
      if (now.day() != last_log.day()) {             // If the day has changed since we last wrote measurements to the SD card, we've rolled-over Midnight, or changed the date,
                                                     // so we start a new extension index and add the last filename to the 'to send' file.
       sprintf(filename, "%04d%02d%02d.%03d", last_log.year(), last_log.month(), last_log.day(), log_index);
       logFileToSend(filename);
       log_index = 0;
      }
      
      last_log = now;
      
      if (now.unixtime() >= (adc_last_time + (int32_t)ADC_UPDATE_RATE_SECS)) {
        logADCSToSD(last_log, log_index);
        clearADCAccumulators();
        adc_last_time = last_log.unixtime();
      }
      if (now.unixtime() >= (opc_last_time + (int32_t)OPC_UPDATE_RATE_SECS)) {
        bool success = OPCN3_readHistogram();
        success = logOPCToSD(last_log, log_index);
        opc_last_time = last_log.unixtime();
      }
      if (now.unixtime() >= (batt_last_time + (int32_t)BATT_UPDATE_RATE_SECS)) {
        bool success = logBattVoltToSD(last_log, log_index);
        batt_last_time = last_log.unixtime();
      }
    }
  }
  return 0;
}

uint8_t readADCS(uint8_t adc_mode) {
  unsigned long temp_timestamp;
  if (adc_mode == 0) {
    startADCReadings();
    ADC_TIMESTAMP = millis();
    adc_mode = 1;
  } else if (adc_mode == 1) {
    temp_timestamp = millis();
    if (temp_timestamp >= (ADC_TIMESTAMP + ADC_INTEGRATION_TIME_MSECS)) {
      accumulateADCReadings();
      ITERATION_INDEX += 1;
      adc_mode = 0;
    } else if (temp_timestamp < ADC_TIMESTAMP) {
      // millis() must have wrapped-around. Check how long has actually elapsed since the last uc_timestamp by adding-up
      // how much was left to elapse before millis() wrapped-around, and how much has elapsed since it wrapped-around.
      unsigned long corrected_uc_timestamp = (4294967296L - ADC_TIMESTAMP) + temp_timestamp;
      if (corrected_uc_timestamp > ADC_INTEGRATION_TIME_MSECS) {
        ADC_TIMESTAMP = corrected_uc_timestamp;
      }
    }
  }
  return adc_mode;
}

bool logADCSToSD(DateTime now, int log_index) {
  bool success = false;
  char string_buffer[13];
  File DATA_FILE;
  // Generate current logging filename.
  sprintf(string_buffer, "%04d%02d%02d.%03d", now.year(), now.month(), now.day(), log_index);
  DATA_FILE = SD.open(string_buffer, O_CREAT | O_WRITE | O_APPEND);
  if (DATA_FILE) {
    success = true;
    strcpy_PF(string_buffer, F("(ADCS)"));
    DATA_FILE.write(string_buffer, 6);
    ultoa(now.unixtime(), string_buffer, 10);
    DATA_FILE.write(string_buffer, 10);
    for (uint8_t measurement_index = 0; measurement_index < ADC_COUNT; measurement_index ++) {
      DATA_FILE.write(',');
      float value = (float)ADC_TOTALS[measurement_index] / (float)ITERATION_INDEX;
      dtostrf(value, 0, 3, string_buffer);
      uint8_t val_length = strlen(string_buffer);
      DATA_FILE.write(string_buffer, val_length);
    }
    DATA_FILE.write("\r\n", 2);
    DATA_FILE.close();
  }
  return success;
}

bool serialListen(bool blocking) {
  char character;
  bool message_to_process = false;
  while(true) {
    while (Serial.available()) {
      character = Serial.read();
      if (INCOMING_STARTED == false) {
        if (character == '>') {
          INCOMING_STARTED = true;
          RECEIVE_INDEX = 0;
          INPUT_MODE = 0;
          INPUT_MESSAGE_LENGTH = 0;
          INPUT_CRC_LENGTH = 0;
        }
      } else if (INCOMING_STARTED == true) {
        if (character == '<') {
          INPUT_MESSAGE_BUFFER[RECEIVE_INDEX] = char(0);
          RECEIVE_INDEX = 0;
          INPUT_MODE = 1;
        } else if (character == '>') {
          RECEIVE_INDEX = 0;
          INPUT_MODE = 0;
          INPUT_MESSAGE_LENGTH = 0;
          INPUT_CRC_LENGTH = 0;
        } else if (character == '\0') {
          INPUT_CRC_BUFFER[RECEIVE_INDEX] = char(0);
          blocking = false;
          message_to_process = true;
          INCOMING_STARTED = false;
          break;
        } else {
          if (INPUT_MODE == 0) {
            INPUT_MESSAGE_BUFFER[RECEIVE_INDEX] = character;
            RECEIVE_INDEX += 1;
            INPUT_MESSAGE_LENGTH += 1;
          } else if (INPUT_MODE == 1) {
            INPUT_CRC_BUFFER[RECEIVE_INDEX] = character;
            RECEIVE_INDEX += 1;
            INPUT_CRC_LENGTH += 1;
          }
          if ((RECEIVE_INDEX == (MAX_INPUT)) && (INPUT_MODE = 0)) {
            RECEIVE_INDEX = 0;
            INPUT_MESSAGE_LENGTH = 0;
          }
          if ((RECEIVE_INDEX == 4) && (INPUT_MODE == 1)) {
            RECEIVE_INDEX = 0;
            INPUT_CRC_LENGTH = 0;
          }
        }
      }
    }
    if (!blocking) {
      break;
    }
  }
  if (message_to_process == true) {
    byte message_crc_value = CRC8(INPUT_MESSAGE_BUFFER, INPUT_MESSAGE_LENGTH);
    byte crc_check_value = atoi(INPUT_CRC_BUFFER);
    CRC_SUCCESS = false;
    if (message_crc_value == crc_check_value) {
      CRC_SUCCESS = true;
    }
  }
  return message_to_process;
}

void serialSpeak(const char * characters) {
  const char * starting_addr = characters;
  Serial.print('>');
  int message_length = 0;
  for (int n = 0; *characters != '\0'; characters ++) {
    Serial.print(*characters);
    message_length ++;
  }
  Serial.print('<');
  byte crc_check_value = CRC8(starting_addr, message_length);
  Serial.print(crc_check_value);
  Serial.print('\0');
  return 0;
}

void serialSpeak(const __FlashStringHelper* characters) {
  int len = 0;
  for (const char PROGMEM *ptr = (const char PROGMEM *)characters; pgm_read_byte(ptr) != '\0'; ptr ++) {
    len ++;
  }
  char string_buffer[len + 1];
  memcpy_P(string_buffer, characters, len);
  string_buffer[len] = '\0';
  serialSpeak(string_buffer);
  return 0;
}

int parseMessage(bool message_to_process, byte message_type) {
  // Compare inbound message against available device commands and if any match, return new device mode.
  int new_mode = 0;
  if (message_to_process == true) {
    if (message_type == 0) {
      for (int i = 0; i < NUM_COMMANDS; i ++) {
        if (strncmp(COMMAND_STRINGS[i], INPUT_MESSAGE_BUFFER, COMMAND_LENGTHS[i]) == 0) {
          new_mode = i + 1;
        }
      }
    } else if (message_type == 1) {
      for (int i = 0; i < 6; i ++) {
        if (strncmp(TIME_HEADER_STRINGS[i], INPUT_MESSAGE_BUFFER, 2) == 0) {
          new_mode = i + 1;
        }
      }
    } else if (message_type == 2) {
      for (int i = 0; i < NUM_SPOOL_COMMANDS; i ++) {
        if (strncmp(SPOOL_COMMAND_STRINGS[i], INPUT_MESSAGE_BUFFER, SPOOL_COMMAND_LENGTHS[i]) == 0) {
          new_mode = i + 1;
        }
      }
    } else if (message_type == 3) {
      for (int i = 0; i < NUM_PARAM_STRINGS; i ++) {
        if (strncmp(PARAM_HEADER_STRINGS[i], INPUT_MESSAGE_BUFFER, 2) == 0) {
          new_mode = i + 1;
        }
      }
    }
  }
  return new_mode;
}

void applyCommand(int new_mode) {
  // Change device behaviour to reflect most recent inbound command.
  switch (new_mode) {
    case 0:
      break;
    case 1:
      setTime();
      break;
    case 2:
      spoolFiles();
      break;
    case 3:
      setParameter();
      break;
  }
  return 0;
} 

void setParameter() {
  bool message_to_process = false;
  byte message_type = 0;
  message_to_process = serialListen(false);
  if ((message_to_process == true) && (CRC_SUCCESS == true)) {
    if (PARAM_COMMAND_MODE == 0) {
      message_type = 0;
      byte mode = parseMessage(message_to_process, message_type);         // Parse message against base commands.
      if (mode != 0) {                                                    // If RPI is still sending CP command, must not have received last ack from arduino...
        COMMAND_MODE = 0;                                                     // Break out of CP command mode.
        serialSpeak(F("sp1"));                                             // Send nack to arduino so CA command is re-sent.
      } else {
        message_type = 3;
        byte new_mode =  parseMessage(message_to_process, message_type);  // Parse message against parameter headers.
        if (new_mode == 0) {                                              // If not recognised...
          serialSpeak(F("sp2"));                                           // Send nack to RPI so parameter header is re-sent.
        } else if (new_mode != 0) {                                       // We have received a recognised parameter header
          serialSpeak(F("ak"));                                            // Send ack to RPI.
          PARAM_COMMAND_MODE = new_mode;                                      // Set the appropriate parameter change mode.
        }
      }
    } else if ((PARAM_COMMAND_MODE == 1) || (PARAM_COMMAND_MODE == 2) || (PARAM_COMMAND_MODE == 3)) {
      message_type = 3;
      byte mode = parseMessage(message_to_process, message_type);         // Parse message against parameter headers.
      if (mode != 0) {                                                    // If RPI is still sending a parameter header, must not have received last ack from arduino...
        serialSpeak(F("ak"));                                              // Send ack to RPI so parameter value is sent.
      } else {                                                            // We have received a parameter value to apply.
        if (PARAM_COMMAND_MODE == 1) {                                        // If we are changing the ADC averaging period.
          uint16_t new_adc_update_rate = atoi(INPUT_MESSAGE_BUFFER);              // Get the new value from the input buffer.
                                                                                      // If atoi() cannot perform a valid conversion, it returns zero, so the following
                                                                                      // conditional will catch these too.
          if (new_adc_update_rate < 1) {                                          // Cap the value between 1 and 60 seconds.
            new_adc_update_rate = 1;
          } else if (new_adc_update_rate > 60) {
            new_adc_update_rate = 60;
          }
          ADC_UPDATE_RATE_SECS = new_adc_update_rate;                             // Apply the new value.
          const __FlashStringHelper* flash_string = F("ADC averaging period set to ");
          char msg_string[strlen_PF(flash_string) + 3];
          strcpy_PF(msg_string, flash_string);
          char value_string[3];
          utoa(ADC_UPDATE_RATE_SECS, value_string, 10);
          strcat(msg_string, value_string);
          logTelemetry(msg_string);
        } else if (PARAM_COMMAND_MODE == 2) {                                 // If we are changing the OPC integration time.
          uint8_t new_opc_update_rate = atoi(INPUT_MESSAGE_BUFFER);               // Get the new value from the input buffer.
          if (new_opc_update_rate < 1) {                                          // Cap the value between 1 and 60 seconds.
            new_opc_update_rate = 1;
          } else if (new_opc_update_rate > 60) {
            new_opc_update_rate = 60;
          }
          OPC_UPDATE_RATE_SECS = new_opc_update_rate;                             // Apply the new value.
          const __FlashStringHelper* flash_string = F("OPC integration time set to ");
          char msg_string[strlen_PF(flash_string) + 3];
          strcpy_PF(msg_string, flash_string);
          char value_string[3];
          utoa(OPC_UPDATE_RATE_SECS, value_string, 10);
          strcat(msg_string, value_string);
          logTelemetry(msg_string);
        } else if (PARAM_COMMAND_MODE == 3) {                                        // If we are changing the web update rate.
          uint16_t new_web_update_rate = atoi(INPUT_MESSAGE_BUFFER);              // Get the new value from the input buffer.
                                                                                      // If atoi() cannot perform a valid conversion, it returns zero, so the following
                                                                                      // conditional will catch these too.
          if (new_web_update_rate < 5) {                                          // Cap the value between 1 and 60 seconds.
            new_web_update_rate = 5;
          } else if (new_web_update_rate > 240) {
            new_web_update_rate = 240;
          }
          RPI_UPDATE_RATE_MINS = new_web_update_rate;                             // Apply the new value.
          const __FlashStringHelper* flash_string = F("RPI update rate set to ");
          char msg_string[strlen_PF(flash_string) + 4];
          strcpy_PF(msg_string, flash_string);
          char value_string[4];
          utoa(ADC_UPDATE_RATE_SECS, value_string, 10);
          strcat(msg_string, value_string);
          logTelemetry(msg_string);
        }
        PARAM_COMMAND_MODE = 4;
        serialSpeak(F("fs"));
      }
    } else if (PARAM_COMMAND_MODE == 4) {
      message_type = 2;
      byte mode = parseMessage(message_to_process, message_type);         // Parse message against spool commands.
      if (mode == 1) {                                                    // If RPI sends ack.
        serialSpeak(F("ak"));                                            // Send ack to RPI and we are done.
        COMMAND_MODE = 0;                                                   // Break out of parameter change mode.
      } else {                                                            // If otherwise not recognised.
        serialSpeak(F("sp3"));                                            // Send nack to RPI.
      }
    }
    for (int j = 0; j < MAX_INPUT; j ++) {
      INPUT_MESSAGE_BUFFER[j] = '\0';
    }
    COMMAND_START_TIMESTAMP = millis();
  } else if ((message_to_process == true) && (CRC_SUCCESS == false)) {  // Whatever it was arrived garbled, send an nak to the RPI.
    serialSpeak(F("sp4"));                                                // Send nack to RPI.
    for (int j = 0; j < MAX_INPUT; j ++) {
      INPUT_MESSAGE_BUFFER[j] = '\0';
    }
    COMMAND_START_TIMESTAMP = millis();
  }
  checkCommandTimeout();
  return 0;
}

void setTime() {
  bool message_to_process = false;
  int mode = 0;
  byte message_type = 0;
  
  message_to_process = serialListen(false);
  if ((message_to_process == true) && (CRC_SUCCESS == true)) {
    if (TIME_COMMAND_MODE == 0) {                                     // If we are waiting to receive a time header...
      message_type = 0;                                                 // Parse message against base commands.
      mode = parseMessage(message_to_process, message_type);            // 0 = No change, 1 = Set time (ST), 2 = Spool data (TX), 3 = Set ADC averaging period (CA).
      if (mode != 0) {                                                  // If RPI is still sending ST command, must not have received last ack from arduino...
        COMMAND_MODE = 0;                                                 // Break out of ST command mode.
        serialSpeak(F("m2"));                                          // Send nack to arduino so ST command is re-sent.
      } else if (mode == 0) {                                           // If RPI is not still sending ST command, must have received last ack from arduino...
        message_type = 1;                                                 // Parsing message against time headers.
        mode = parseMessage(message_to_process, message_type);            // 0 = Not recognised, 1 = Year, 2 = Month, 3 = Day, 4 = Hour, 5 = Minute, 6 = Second.
        if (mode != 0) {                                                  // If time header successfully parsed...
          COMMAND_STAGE = mode - 1;
          TIME_COMMAND_MODE = 1;                                            // Switch to receiving time values instead of time headers.
          serialSpeak(F("ht"));                                          // Send ack to RPI.
        } else if (mode == 0) {                                           // If time header not recognised...
          serialSpeak(F("m3"));                                          // Send nack to RPI so header is re-sent.
        }
      }
    } else if (TIME_COMMAND_MODE == 1) {                              // If we are waiting to receive a time value...
      message_type = 1;                                                 // Parse message against time headers.
      mode = parseMessage(message_to_process, message_type);            // 0 = Not recognised, 1 = Year, 2 = Month, 3 = Day, 4 = Hour, 5 = Minute, 6 = Second.
      if (mode != 0) {                                                  // If RPI is still sending time header, must not have received last ack from arduino...
        TIME_COMMAND_MODE = 0;                                            // Switch to receiving time headers instead of time values.
        serialSpeak(F("m4"));                                          // Send nack to RPI.
      } else if (mode == 0) {                                           // If RPI is sending values...
        TIMESTAMP_BUFFER[COMMAND_STAGE] = atoi(INPUT_MESSAGE_BUFFER);     // Convert value from char array to int.
        TIME_COMMAND_MODE = 0;                                            // Switch back to receiving time headers.
        if (COMMAND_STAGE == 5) {                                         // If this is the last time value to get...   (assumes headers are dispatched in order!)
          RTC.adjust(DateTime(TIMESTAMP_BUFFER[0], TIMESTAMP_BUFFER[1], TIMESTAMP_BUFFER[2], TIMESTAMP_BUFFER[3], TIMESTAMP_BUFFER[4], TIMESTAMP_BUFFER[5]));   // Set the time.
          serialSpeak(F("fs"));                                          // Send final ack to RPI.
          logTelemetry(F("Time update completed"));                       // Log the time update.
          TIME_COMMAND_MODE = 2;                                            // Switch to time update handshake mode.
        } else if (COMMAND_STAGE < 5) {                                   // If this is not the last time value to get...
          serialSpeak(F("ht"));                                          // Send ack to RPI.
        }
      }
    } else if (TIME_COMMAND_MODE == 2) {                              // If we are handshaking.
      message_type = 2;                                                 // Parse message against spool commands.
      mode = parseMessage(message_to_process, message_type);            // 
      if (mode == 1) {                                                  // If RPI sent ack,
        serialSpeak(F("ak"));                                          // Send ack to RPI and we are done.
        COMMAND_MODE = 0;                                                 // Break out of ST command mode.
      } else {                                                          // If otherwise not recognised.
        serialSpeak(F("sp3"));                                         // Send nack to RPI.
      }
    }
    for (int j = 0; j < MAX_INPUT; j ++) {
      INPUT_MESSAGE_BUFFER[j] = '\0';
    }
    COMMAND_START_TIMESTAMP = millis();
  } else if ((message_to_process == true) && (CRC_SUCCESS == false)) {  // Whatever it was arrived garbled, send an nak to the RPI.
    serialSpeak(F("m5"));                                              // Send nack to RPI.
    for (int j = 0; j < MAX_INPUT; j ++) {
      INPUT_MESSAGE_BUFFER[j] = '\0';
    }
    COMMAND_START_TIMESTAMP = millis();
  }
  checkCommandTimeout();
  return 0;
}

bool spoolFiles() {
  bool success = true;
  if (SPOOL_COMMAND_MODE == 0) {
    // Open SEND file and read the next 13 characters from it into a filename buffer.
    File SEND_FILE;
    char string_buffer[9];
    strcpy_PF(string_buffer, F("SEND.TXT"));
    SEND_FILE = SD.open(string_buffer);
    if (SEND_FILE) {
      unsigned long send_file_size = SEND_FILE.size();
      if (SEND_FILE_CURSOR < send_file_size) {                               // If we haven't reached the end of the send file list...
        SEND_FILE.seek(SEND_FILE_CURSOR);
        char character = '\0';
        int index = 0;
        while (character != '\n') {
          character = SEND_FILE.read();
          if ((character != '\r') && (character != '\n')) {
            FILENAME_BUFFER[index] = character;                             // Store the filename in a global buffer.
            index ++;
          }
          SEND_FILE_CURSOR ++;
        }
        FILENAME_BUFFER[index] = '\0';                                      // Appends terminating \0 to buffers.
        char filename_string[15] = "f}            ";
        for (int j = 0; j < 15; j ++) {
          filename_string[j + 2] = FILENAME_BUFFER[j];
        }
        serialSpeak(filename_string);
        success = true;
        DATA_FILE_CURSOR = 0;                                               // Zero the data file cursor.
        SPOOL_COMMAND_MODE = 1;                                             // Switch to 'Send file index' mode.
      } else {
        success = true;
        serialSpeak(F("fs"));                                                  // No more files in the SEND file list, signal to RPI we are finished.
        SEND_FILE_CURSOR = 0;                                               // Zero the SEND file cursor.
        SPOOL_COMMAND_MODE = 5;                                             // Switch to 'Completion handshake' mode.
      }
      SEND_FILE.close();
      COMMAND_START_TIMESTAMP = millis();
    } else {
      COMMAND_MODE = 0;                                                       // Couldn't open SEND file, flag this and switch back to base command mode.
      success = false;
    }
  } else if (SPOOL_COMMAND_MODE == 1) {
    // Send file extension index value to RPI.
    bool message_to_process = serialListen(false);
    if ((message_to_process == true) && (CRC_SUCCESS == true)) {
      byte message_type = 0;                                                    // Parse message against base commands.
      int mode = parseMessage(message_to_process, message_type);                // 0 = Not recognised, 1 = Set time (ST), 2 = Spool data (TX).
      if (mode == 2) {                                                          // TX received from RPI, must not have correctly received the first filename string.
        char filename_string[15] = "f}            ";                                // Re-send filename buffer.
        for (int j = 0; j < 15; j ++) {
          filename_string[j + 2] = FILENAME_BUFFER[j];
        }
        serialSpeak(filename_string);
      } else {
        message_type = 2;                                                       // Parse message against TX commands.
        int new_mode = parseMessage(message_to_process, message_type);          // 0 = Not recognised, 1 = Acknowledge frame (AK), 2 = Acknowledge resend (AR), 3 = TX completed (CC).
        if (new_mode == 0) {                                                    // Response from RPI not recognised.
          serialSpeak(F("cr"));                                                     // Confirm resend.
        } else if (new_mode == 1) {                                             // AK received from RPI, filename string correctly received.
          SPOOL_COMMAND_MODE = 2;                                                   // Switch to 'load data from current file' mode.
        } else if (new_mode == 2) {                                             // AR received from RPI, filename failed CRC check.
          char filename_string[15] = "f}            ";                                // Re-send filename buffer.
          for (int j = 0; j < 15; j ++) {
            filename_string[j + 2] = FILENAME_BUFFER[j];
          }
          serialSpeak(filename_string);
        }
      }
      COMMAND_START_TIMESTAMP = millis();
    } else if ((message_to_process == true) && (CRC_SUCCESS == false)) {        // If message from RPI fails its CRC check, we don't know if the RPI received the filename string
      serialSpeak(F("cr"));                                                         // Confirm resend.
      COMMAND_START_TIMESTAMP = millis();
    }
  } else if (SPOOL_COMMAND_MODE == 2) {
    // Open filename in buffer and read the next data frame into a data buffer.
    File DATA_FILE;
    DATA_FILE = SD.open(FILENAME_BUFFER);
    if (DATA_FILE) {
      unsigned long data_file_size = DATA_FILE.size();
      if (DATA_FILE_CURSOR < (data_file_size - 1L)) {                                // If we haven't reached the end of the DATA file...
        DATA_FILE.seek(DATA_FILE_CURSOR);
        uint8_t packet_size = OUTPUT_PACKET_LENGTH;
        if ((data_file_size - DATA_FILE_CURSOR) < packet_size) {
          packet_size = data_file_size - DATA_FILE_CURSOR;
        }
        DATA_FILE.read(OUTPUT_BUFFER, packet_size);
        OUTPUT_BUFFER[packet_size] = '\0';                                            // Appends terminating \0 to buffer.
        DATA_FILE_CURSOR += packet_size;
        SPOOL_COMMAND_MODE = 3;                                             // Switch to 'Send data frame' mode.
      } else {                                                                // We've hit the end of the current DATA file, queue-up the next DATA file.
        SPOOL_COMMAND_MODE = 0;
      }
      DATA_FILE.close();
    } else {
      COMMAND_MODE = 0;                                                       // Couldn't open DATA file, flag this and switch back to base command mode.
      success = false;
    }
    COMMAND_START_TIMESTAMP = millis();
  } else if (SPOOL_COMMAND_MODE == 3) {
    // Send the data frame to the RPI.
    serialSpeak(OUTPUT_BUFFER);
    SPOOL_COMMAND_MODE = 4;                                                     // Switch to 'waiting for ack' mode.
    COMMAND_START_TIMESTAMP = millis();
  } else if (SPOOL_COMMAND_MODE == 4) {
    // Data transmission handshaking with RPI.
    bool message_to_process = serialListen(false);
    if ((message_to_process == true) && (CRC_SUCCESS == true)) {
      byte message_type = 0;                                                  // Parse message against base commands.
      int mode = parseMessage(message_to_process, message_type);              // 0 = No change, 1 = Set time (ST), 2 = Spool data (TX).
      if (mode != 0) {                                                        // If RPI is still sending a TX command, first data frame arrived garbled.
        COMMAND_MODE = 0;                                                   // Switch back to base command mode.
      } else {
        message_type = 2;                                                   // Otherwise, parse message against TX commands.
        byte new_mode = parseMessage(message_to_process, message_type);     // 0 = Not recognised, 1 = Acknowledge frame (AK), 2 = Acknowledge resend (AR), 3 = TX completed (CC).
        if (new_mode == 0) {                                                // CRC check passed but command not recognised, confirm resend.
          serialSpeak(F("cr"));
          SPOOL_COMMAND_MODE = 4;
        } else if (new_mode == 1) {                                         // AK received from RPI...
          if (EOF_FLAG == false) {
            SPOOL_COMMAND_MODE = 2;                                     // --- We haven't reached the end of the data file yet, so queue-up the next data frame.
          } else {
            SPOOL_COMMAND_MODE = 0;                                     // --- We have reached the end of the data file, so queue-up the next data file.
            EOF_FLAG = false;
          }
        } else if (new_mode == 2) {                                         // AR received from RPI, resend the output buffer.
          SPOOL_COMMAND_MODE = 3;
        }
      }
      COMMAND_START_TIMESTAMP = millis();
    } else if ((message_to_process == true) && (CRC_SUCCESS == false)) {        // If message from RPI fails its CRC check, confirm resend.
      serialSpeak(F("cr"));
      SPOOL_COMMAND_MODE = 4;
      COMMAND_START_TIMESTAMP = millis();
    }
  } else if (SPOOL_COMMAND_MODE == 5) {
    // Transmission complete handshaking with RPI.
    bool message_to_process = serialListen(false);
    if ((message_to_process == true) && (CRC_SUCCESS == true)) {
      byte message_type = 2;                                                  // Parse message against TX commands.
      int mode = parseMessage(message_to_process, message_type);              // 0 = Not recognised, 1 = Acknowledge frame (AK), 2 = Acknowledge resend (AR), 3 = TX completed (CC).
      if (mode == 0) {                                                        // CRC check passed but command not recognised, confirm completion.
        serialSpeak(F("fs"));
      } else if (mode == 3) {                                                 // CC received from RPI, transmission complete!
        serialSpeak(F("cc"));                                                  // Send final confirmation.
        char string_buffer[9];
        strcpy_PF(string_buffer, F("SEND.TXT"));
        SD.remove(string_buffer);
        logTelemetry(F("Data transmission complete"));
        COMMAND_MODE = 0;                                                   // Switch back to base command mode.
      }
      COMMAND_START_TIMESTAMP = millis();
    } else if ((message_to_process == true) && (CRC_SUCCESS == false)) {        // If message from RPI fails its CRC check, confirm completion.
      serialSpeak(F("fs"));
      COMMAND_START_TIMESTAMP = millis();
    }
  }
  checkCommandTimeout();
  return success;
}

bool checkCommandTimeout() {
  bool timeout = false;
  unsigned long current_timestamp = millis();
  if (current_timestamp > (COMMAND_START_TIMESTAMP + 1000L)) {
    COMMAND_MODE = 0;
    serialSpeak(F("to"));
    logTelemetry(F("Command timeout"));
    timeout = true;
  } else if (current_timestamp < COMMAND_START_TIMESTAMP) {
    // millis() must have overflowed. Correct for this.
    unsigned long corrected_timestamp = (4294967296L - COMMAND_START_TIMESTAMP) + current_timestamp;
    if (corrected_timestamp > 1000L) {
      COMMAND_START_TIMESTAMP = corrected_timestamp;
    }
  }
  return timeout;
}

//CRC-8 - based on the CRC8 formulas by Dallas/Maxim
//code released under the therms of the GNU GPL 3.0 license
byte CRC8(const byte *data, byte len) {
  byte crc = 0x00;
  while (len--) {
    byte extract = *data++;
    for (byte tempI = 8; tempI; tempI--) {
      byte sum = (crc ^ extract) & 0x01;
      crc >>= 1;
      if (sum) {
        crc ^= 0x8C;
      }
      extract >>= 1;
    }
  }
  return crc;
}

bool initialiseChannel(uint8_t channel) {
  // Initialise Channel n.
  i2cSelectChannel(channel);
  // Initialise ADCs.
  for (uint8_t i = 0; i < ADCS_PER_CHANNEL[channel]; i ++) {
    ADCS[i].begin();
    ADCS[i].setGain(GAIN_ONE);    // Set ADC PGA to +/- 4.096V
  }
  return 0;
}

uint8_t i2cSelectChannel(uint8_t channel) {
  Wire.beginTransmission(0x70);                               // 0x70 is the address of the i2c mux.
  Wire.write(1 << channel);                                   // Switch channel.
  return Wire.endTransmission();
}

void startADCReadings() {
  for (uint8_t i = 0; i < 4; i ++) {
    i2cSelectChannel(i);
    for (uint8_t j = 0; j < ADCS_PER_CHANNEL[i]; j ++) {
      ADCS[j].NB_Start_ADC_Differential_0_1();
    }
  }
  return 0;
}

void accumulateADCReadings() {
  uint8_t adcs_to_this_point = 0;
  for (uint8_t i = 0; i < 4; i ++) {
    i2cSelectChannel(i);
    for (uint8_t j = 0; j < ADCS_PER_CHANNEL[i]; j ++) {
      int16_t new_measurement = ADCS[j].NB_Read_ADC_Differential();
      ADC_TOTALS[adcs_to_this_point] += (int32_t)new_measurement;
      adcs_to_this_point += 1;
    }
  }
  return 0;
}

void clearADCAccumulators() {
  for (uint8_t i = 0; i < ADC_COUNT; i ++) {
    ADC_TOTALS[i] = 0;
  }
  ITERATION_INDEX = 0;
  return 0;
}

byte getReadyResponse(byte command) {
  SPI.beginTransaction (SPISettings (300000, MSBFIRST, SPI_MODE1));
  byte resp;
  digitalWrite(_CS, LOW);
  for (int i = 0; i < 30; i ++) {
    resp = SPI.transfer(command);
    if (resp == 0xF3) {
      delayMicroseconds(10);
      break;
    } else {
      delay(5);
    }
  }
  return resp;
}

bool OPCN3_setOPCState(byte mode, bool on) {
  // Mode byte 0 = fan, 1 = laser.
  bool success = false;
  byte ready_response;
  while (success == false) {
    ready_response = getReadyResponse(0x03);
    if (ready_response == 0xF3) {
      if (mode == 0) {
        if (on == true) {
          byte response = command_fanOn();
          if (response == 0x03) {
            success = true;
          }
        } else if (on == false) {
          byte response = command_fanOff();
          if (response == 0x03) {
            success = true;
          }
        }
      } else if (mode == 1) {
        if (on == true) {
          byte response = command_laserOn();
          if (response == 0x03) {
            success = true;
          }
        } else if (on == false) {
          byte response = command_laserOff();
          if (response == 0x03) {
            success = true;
          }
        }
      }
    } else if (ready_response == 0x31) {
      digitalWrite(_CS, HIGH);
      delay(2500);
    } else {
      digitalWrite(_CS, HIGH);
      SPI.endTransaction();
      delay(5000);
      SPI.beginTransaction (SPISettings (300000, MSBFIRST, SPI_MODE1));    }
  }
  if (success == false) {
    //
  }
  SPI.endTransaction();
  return success;
}

byte command_fanOn() {
  byte resp = SPI.transfer(0x03);
  digitalWrite(_CS, HIGH);
  return resp;
}

byte command_fanOff() {
  byte resp = SPI.transfer(0x02);
  digitalWrite(_CS, HIGH);
  return resp;
}

byte command_laserOn() {
  byte resp = SPI.transfer(0x07);
  digitalWrite(_CS, HIGH);
  return resp;
}

byte command_laserOff() {
  byte resp = SPI.transfer(0x06);
  digitalWrite(_CS, HIGH);
  return resp;
}

bool OPCN3_readHistogram() {
  bool success = false;
  byte ready_response = getReadyResponse(0x30);
  
  if (ready_response == 0xF3) {
    for (byte spi_index = 0; spi_index < 86; spi_index ++) {
      SPI_in[spi_index] = SPI.transfer(0x01);
      delayMicroseconds(10);
    }
    digitalWrite(_CS, HIGH);
    SPI.endTransaction();
    success = true;
  } else {
    digitalWrite(_CS, HIGH);
    SPI.endTransaction();
  }
  return success;
}

bool logOPCToSD(DateTime now, int log_index) {
  bool success = false;
  unsigned char i;
  unsigned int *pUInt16;
  float *pFloat;
  float Afloat;

  File DATA_FILE;
  char string_buffer[13];
  // Generate current logging filename.
  sprintf(string_buffer, "%04d%02d%02d.%03d", now.year(), now.month(), now.day(), log_index);
  DATA_FILE = SD.open(string_buffer, O_CREAT | O_WRITE | O_APPEND);
  
  if (DATA_FILE) {
    strcpy_PF(string_buffer, F("(OPC)"));
    DATA_FILE.write(string_buffer, 5);
    ultoa(now.unixtime(), string_buffer, 10);
    DATA_FILE.write(string_buffer, 10);
    DATA_FILE.write(',');
    
    //Histogram bins (UInt16) x16
    for (i=0; i<48; i+=2) {
      pUInt16 = (unsigned int *)&SPI_in[i];
      utoa(*pUInt16, string_buffer, 10);
      DATA_FILE.write(string_buffer, strlen(string_buffer));
      DATA_FILE.write(',');
    }
    
    //MToF bytes (UInt8) x4
    for (i=48; i<52; i++) {
      Afloat = (float)SPI_in[i];
      Afloat /= 3; //convert to us
      dtostrf(Afloat, 0, 2, string_buffer);
      DATA_FILE.write(string_buffer, strlen(string_buffer));
      DATA_FILE.write(',');
    }
    
    //Sampling period(s) (UInt16) x1
    pUInt16 = (unsigned int *)&SPI_in[52];
    dtostrf((float)*pUInt16/100, 0, 3, string_buffer);
    DATA_FILE.write(string_buffer, strlen(string_buffer));
    DATA_FILE.write(',');
    
    //SFR (UInt16) x1
    pUInt16 = (unsigned int *)&SPI_in[54];
    dtostrf((float)*pUInt16/100, 0, 3, string_buffer);
    DATA_FILE.write(string_buffer, strlen(string_buffer));
    DATA_FILE.write(',');
    
    //Temperature (UInt16) x1
    pUInt16 = (unsigned int *)&SPI_in[56];
    dtostrf(ConvSTtoTemperature(*pUInt16), 0, 1, string_buffer);
    DATA_FILE.write(string_buffer, strlen(string_buffer));
    DATA_FILE.write(',');
    
    //Relative humidity (UInt16) x1
    pUInt16 = (unsigned int *)&SPI_in[58];
    dtostrf(ConvSRHtoRelativeHumidity(*pUInt16), 0, 1, string_buffer);
    DATA_FILE.write(string_buffer, strlen(string_buffer));
    DATA_FILE.write(',');
    
    //PM values(ug/m^3) (4-byte float) x3
    for (i=60; i<72; i+=4) {
      pFloat = (float *)&SPI_in[i];
      dtostrf(*pFloat, 0, 3, string_buffer);
      DATA_FILE.write(string_buffer, strlen(string_buffer));
      DATA_FILE.write(',');
    }
    
    //Reject count Glitch (UInt16) x1
    pUInt16 = (unsigned int *)&SPI_in[72];
    utoa(*pUInt16, string_buffer, 10);
    DATA_FILE.write(string_buffer, strlen(string_buffer));
    DATA_FILE.write(',');
    
    //Reject count LongTOF (UInt16) x1
    pUInt16 = (unsigned int *)&SPI_in[74];
    utoa(*pUInt16, string_buffer, 10);
    DATA_FILE.write(string_buffer, strlen(string_buffer));
    DATA_FILE.write(',');
    
    //Reject count Ratio (UInt16) x1
    pUInt16 = (unsigned int *)&SPI_in[76];
    utoa(*pUInt16, string_buffer, 10);
    DATA_FILE.write(string_buffer, strlen(string_buffer));
    DATA_FILE.write(',');
    
    //Reject count OutOfRange (UInt16) x1
    pUInt16 = (unsigned int *)&SPI_in[78];
    utoa(*pUInt16, string_buffer, 10);
    DATA_FILE.write(string_buffer, strlen(string_buffer));
    DATA_FILE.write(',');
    
    //Fan rev count (UInt16) x1 (Not using)
    //AddDelimiter(port);
    //pUInt16 = (unsigned int *)&SPI_in[80];
    //port.print(*pUInt16, DEC);
    
    //Laser status (UInt16) x1
    pUInt16 = (unsigned int *)&SPI_in[82];
    utoa(*pUInt16, string_buffer, 10);
    DATA_FILE.write(string_buffer, strlen(string_buffer));
    DATA_FILE.write(',');
    
    //Checksum (UInt16) x1
    pUInt16 = (unsigned int *)&SPI_in[84];
    utoa(*pUInt16, string_buffer, 10);
    DATA_FILE.write(string_buffer, strlen(string_buffer));
    DATA_FILE.print("\r\n");;
    
    //Compare recalculated Checksum with one sent
    if (*pUInt16 != MODBUS_CalcCRC(SPI_in, 84)) { //if checksums aren't equal
      DATA_FILE.println(F("(OPC)Checksum error in line above!"));
    }
    DATA_FILE.close();
    success = true;
  } else {
    success = false;
  }
  return success;
}

//Convert SHT31 ST output to Temperature (C)
float ConvSTtoTemperature (unsigned int ST)
{
  return -45 + 175*(float)ST/65535;
}

//Convert SHT31 SRH output to Relative Humidity (%)
float ConvSRHtoRelativeHumidity (unsigned int SRH)
{
  return 100*(float)SRH/65535;
}

unsigned int MODBUS_CalcCRC(unsigned char data[], unsigned char nbrOfBytes)
{
  #define POLYNOMIAL_MODBUS 0xA001 //Generator polynomial for MODBUS crc
  #define InitCRCval_MODBUS 0xFFFF //Initial CRC value

  unsigned char _bit; // bit mask
  unsigned int crc = InitCRCval_MODBUS; // initialise calculated checksum
  unsigned char byteCtr; // byte counter

  // calculates 16-Bit checksum with given polynomial
  for(byteCtr = 0; byteCtr < nbrOfBytes; byteCtr++)
  {
    crc ^= (unsigned int)data[byteCtr];
    for(_bit = 0; _bit < 8; _bit++)
    {
      if (crc & 1) //if bit0 of crc is 1
      {
        crc >>= 1;
        crc ^= POLYNOMIAL_MODBUS;
      }
      else
        crc >>= 1;
    }
  }
  return crc;
}

void averageBattVolt() {
  unsigned long temp_timestamp = millis();
  if (temp_timestamp >= (BATT_TIMESTAMP + 50L)) {
    BATT_TOTAL += (uint32_t)analogRead(PIN_BATT_VPLUS);
    BATT_ITERATION_INDEX += 1;
    BATT_TIMESTAMP = millis();
  } else if (temp_timestamp < BATT_TIMESTAMP) {
    // millis() must have wrapped-around. Check how long has actually elapsed since the last uc_timestamp by adding-up
    // how much was left to elapse before millis() wrapped-around, and how much has elapsed since it wrapped-around.
    unsigned long corrected_timestamp = (4294967296L - BATT_TIMESTAMP) + temp_timestamp;
    BATT_TIMESTAMP = corrected_timestamp;
  }
  return 0;
}

bool logBattVoltToSD(DateTime now, int log_index) {
  bool success = false;
  float voltage_raw = ((float)BATT_TOTAL) / ((float)BATT_ITERATION_INDEX);
  float voltage = (4.096 / 1023.0) * voltage_raw * 4.0;
  BATT_ITERATION_INDEX = 0;
  BATT_TOTAL = 0;
  File DATA_FILE;
  char string_buffer[13];
  // Generate current logging filename.
  sprintf(string_buffer, "%04d%02d%02d.%03d", now.year(), now.month(), now.day(), log_index);
  DATA_FILE = SD.open(string_buffer, O_CREAT | O_WRITE | O_APPEND);
  
  if (DATA_FILE) {
    strcpy_PF(string_buffer, F("(BATT)"));
    DATA_FILE.write(string_buffer, 6);
    ultoa(now.unixtime(), string_buffer, 10);
    DATA_FILE.write(string_buffer, 10);
    DATA_FILE.write(',');
    dtostrf(voltage, 0, 3, string_buffer);
    DATA_FILE.write(string_buffer, strlen(string_buffer));
    DATA_FILE.print("\r\n");;
    DATA_FILE.close();
    success = true;
  } else {
    success = false;
  }
  return success;
}
