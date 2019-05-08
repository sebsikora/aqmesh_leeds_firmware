// Date and time functions using a PCF8523 RTC connected via I2C and Wire lib
#include <Wire.h>
//#include <SD.h>
#include "SdFat.h"
#include <RTClib.h>
#include <Adafruit_ADS1015_NB.h>

SdFat SD;
#define SD_CS_PIN 10

Adafruit_ADS1115 ADCS[4] = {Adafruit_ADS1115(0x48),
                            Adafruit_ADS1115(0x49),
                            Adafruit_ADS1115(0x4A),
                            Adafruit_ADS1115(0x4B)};

RTC_PCF8523 RTC;

int RESET_WARNING = 0;
unsigned long UC_TIMESTAMP;
int REPEAT_COUNT = 0;
unsigned long UPDATE_RATE_MSECS = 0;
unsigned long UPDATE_TIMESTAMP = 0;

// ADC Measurement.
float ADC_TOTALS_0[8]={0.0, 0.0, 0.0, 0.0,
                      0.0, 0.0, 0.0, 0.0};
float ADC_TOTALS_1[8]={0.0, 0.0, 0.0, 0.0,
                      0.0, 0.0, 0.0, 0.0};

uint8_t ADCS_PER_CHANNEL[4] = {4, 2, 1, 1};
unsigned long ADC_TIMESTAMP = 0;
unsigned long ADC_INTEGRATION_TIME_MSECS = 63;
uint8_t SAMPLES_PER_ITERATION = 6;

// Inbound message handling.
const int NUM_COMMANDS = 2;
const int MAX_COMMAND_LENGTH = 2;
const char COMMAND_STRINGS[NUM_COMMANDS][MAX_COMMAND_LENGTH] = {"ST", "TX"};
const byte COMMAND_LENGTHS[NUM_COMMANDS] = {2, 2};
const int MAX_INPUT = 9;
byte INPUT_MODE = 0;    // 0 = message, 1 = CRC8 check.
char INPUT_MESSAGE_BUFFER[MAX_INPUT];
char INPUT_CRC_BUFFER[4] = "   ";
byte INPUT_MESSAGE_LENGTH = 0;
byte INPUT_CRC_LENGTH = 0;
int RECEIVE_INDEX = 0;
bool INCOMING_STARTED = false;
int COMMAND_MODE = 0;
int COMMAND_STAGE = 0;
unsigned long COMMAND_START_TIMESTAMP = 0;
int TIMESTAMP_BUFFER[] = {0, 0, 0, 0, 0, 0};
const char TIME_HEADER_STRINGS[6][2] = {"YY", "MM", "DD", "hh", "mm", "ss"};
byte TIME_COMMAND_MODE = 0;     // 0 = Header, 1 = Value.
bool CRC_SUCCESS = false;

// Outbound message handling.
byte SPOOL_COMMAND_MODE = 0;
const int NUM_SPOOL_COMMANDS = 3;
const int MAX_SPOOL_COMMAND_LENGTH = 2;
const char SPOOL_COMMAND_STRINGS[NUM_SPOOL_COMMANDS][MAX_SPOOL_COMMAND_LENGTH] = {"AK", "AR", "CC"};
const byte SPOOL_COMMAND_LENGTHS[NUM_SPOOL_COMMANDS] = {2, 2, 2};
const byte OUTPUT_PACKET_LENGTH = 96;
char OUTPUT_BUFFER[OUTPUT_PACKET_LENGTH + 1];
char FILENAME_BUFFER[13];
char SEND_FILE_INDEX_BUFFER[4];
unsigned long SEND_FILE_CURSOR = 0;
unsigned long DATA_FILE_CURSOR = 0;
bool EOF_FLAG = false;

// Pins
const int PIN_RPI_CONNECTED = 4;
const int PIN_RPI_HARDWARE_RUN = 5;
const int PIN_RTC_FAIL_WARN = 6;
const int PIN_SD_FAIL_WARN = 7;

void setup() {
  // Setup pins.
  pinMode(PIN_RPI_CONNECTED, INPUT_PULLUP);
  pinMode(PIN_RTC_FAIL_WARN, OUTPUT);
  digitalWrite(PIN_RTC_FAIL_WARN, LOW);
  pinMode(PIN_SD_FAIL_WARN, OUTPUT);
  digitalWrite(PIN_SD_FAIL_WARN, LOW);

  Serial.begin(115200);
  delay(500);

  // Initialise Real Time Clock.
  if (! RTC.begin()) {
    //Serial.println(F("Couldn't initialise RTC..."));
    digitalWrite(PIN_RTC_FAIL_WARN, HIGH);
    while (1);
  }
  // INITIALISE SD CARD:
  if (! SD.begin(SD_CS_PIN)) {
    //Serial.println(F("Couldn't initialise SD card..."));
    digitalWrite(PIN_SD_FAIL_WARN, HIGH);
    while (true) {}
  }

  // Initialise ADCs.
  initialiseChannel(0);
  initialiseChannel(1);
  initialiseChannel(2);
  initialiseChannel(3);

  Wire.setClock(800000L);
}

int getLogIndex() {
  char filename[13] = "            ";
  // Determine next file number.
  int log_index = 0;
  DateTime filenow;
  // Get the current time from the RTC.
  filenow = RTC.now();
  bool finished = false;
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
    logTelemetry("AR", 2);
  }
  return log_index;
}

bool logTelemetry(const char * characters, uint8_t buffer_length) {
  bool success = false;
  DateTime lognow;
  File LOG_FILE;
  // Get the current time from the RTC.
  lognow = RTC.now();
  LOG_FILE = SD.open("LOG.CSV", O_CREAT | O_WRITE | O_APPEND);
  if (LOG_FILE) {
    success = true;
    char time_string_buffer[11];
    ultoa(lognow.unixtime(), time_string_buffer, 10);
    LOG_FILE.write(time_string_buffer, 10);
    LOG_FILE.write(',');
    LOG_FILE.write(characters, buffer_length);
    LOG_FILE.write('\r');
    LOG_FILE.write('\n');
    LOG_FILE.close();
  }
  return success;
}

void logTelemetry(const __FlashStringHelper* characters, const int len) {
  char string_buffer[len + 1];
  memcpy_P(string_buffer, characters, len);
  string_buffer[len] = '\0';
  logTelemetry(string_buffer, len);
  return 0;
}

bool logFileToSend(const char * characters) {
  bool success = false;
  File SEND_FILE;
  SEND_FILE = SD.open("SEND.TXT", O_CREAT | O_WRITE | O_APPEND);
  if (SEND_FILE) {
    success = true;
    for (int n = 0; *characters != '\0'; characters ++) {
      SEND_FILE.print(*characters);
    }
    SEND_FILE.print("\r\n");
    SEND_FILE.close();
  }
  return success;
}

void loop() {
  DateTime now = RTC.now();
  UC_TIMESTAMP = millis();
  int log_index = getLogIndex();
  char filename[13] = "            ";
  uint8_t adc_mode = 0;
  bool measuring = false;
  uint8_t iteration_index = 0;
  unsigned long test_timestamp = 0;
  
  while (true) {
    // Listen for any new incoming characters over serial connection, set new mode appropriately.
    bool message_to_process;
    int mode;
    byte message_type;
    unsigned long temp_timestamp;
    
    if (COMMAND_MODE == 0) {      // Idle, waiting for commands...
      message_to_process = serialListen(false);
      if (message_to_process == true) {
        if (CRC_SUCCESS == true) {
          message_type = 0;
          mode = parseMessage(message_to_process, message_type);          // 0 = No change, 1 = Set time...
          if (mode == 0) {
            serialSpeak("fl");
            COMMAND_MODE = 0;
          } else {
            if (mode == 1) {
              serialSpeak(F("ak"), 2);
              COMMAND_MODE = 1;
              TIME_COMMAND_MODE = 0;
              logTelemetry(F("TUS"), 3);
            }
            if (mode == 2) {
              now = RTC.now();
              sprintf(filename, "%04d%02d%02d.%03d", now.year(), now.month(), now.day(), log_index);              // These lines add the current file to the 'to-send' list
              logFileToSend(filename);                                                                            // Then increase the index so the next data log will create
              log_index ++;                                                                                       // a new logging file (todays date, index incremented).
              COMMAND_MODE = 2;
              SPOOL_COMMAND_MODE = 0;
              logTelemetry(F("TXS"), 3);
            }
            COMMAND_START_TIMESTAMP = millis();
          }
        } else if (CRC_SUCCESS == false) {
          serialSpeak(F("fl"), 2);
          COMMAND_MODE = 0;
        }
        for (int j = 0; j < MAX_INPUT; j ++) {
          INPUT_MESSAGE_BUFFER[j] = '0';
        }
      }
    } else {
      applyCommand(COMMAND_MODE);
    }
    
    DateTime last_time = now;
    now = RTC.now();
    if (measuring == false) {
      if (now.unixtime() > last_time.unixtime()) {
        if (now.day() != last_time.day()) {        // If the day has changed since we last wrote measurements to the SD card, we've rolled-over Midnight, or changed the date,
                                                  // so we start a new extension index and add the last filename to the 'to send' file.
         sprintf(filename, "%04d%02d%02d.%03d", last_time.year(), last_time.month(), last_time.day(), log_index);
         logFileToSend(filename);
         log_index = 0;
        }
        measuring = true;
        adc_mode = 0;
        //test_timestamp = millis();
      }
    } else if (measuring == true) {
      if (adc_mode == 0) {
        clearADCAccumulators();
        iteration_index = 0;
        adc_mode = 1;
      } else if (adc_mode == 1) {
        startADCReadings(0);
        ADC_TIMESTAMP = millis();
        adc_mode = 2;
      } else if (adc_mode == 2) {
        temp_timestamp = millis();
        if (temp_timestamp >= (ADC_TIMESTAMP + ADC_INTEGRATION_TIME_MSECS)) {
          accumulateADCReadings(0);
          adc_mode = 3;
          //Serial.println(millis() - test_timestamp);
        } else if (temp_timestamp < ADC_TIMESTAMP) {
          // millis() must have wrapped-around. Check how long has actually elapsed since the last uc_timestamp by adding-up
          // how much was left to elapse before millis() wrapped-around, and how much has elapsed since it wrapped-around.
          unsigned long corrected_uc_timestamp = (4294967296L - ADC_TIMESTAMP) + temp_timestamp;
          if (corrected_uc_timestamp > ADC_INTEGRATION_TIME_MSECS) {
            ADC_TIMESTAMP = corrected_uc_timestamp;
          }
        }
      } else if (adc_mode == 3) {
        startADCReadings(1);
        ADC_TIMESTAMP = millis();
        adc_mode = 4;
      } else if (adc_mode == 4) {
        temp_timestamp = millis();
        if (temp_timestamp >= (ADC_TIMESTAMP + ADC_INTEGRATION_TIME_MSECS)) {
          accumulateADCReadings(1);
          //Serial.println(millis() - test_timestamp);
          if (iteration_index == (SAMPLES_PER_ITERATION - 1)) {
            //Serial.println(millis() - test_timestamp);
            averageADCAccumulators();
            logToSD(now, log_index);
            measuring = false;
            //Serial.println(millis() - test_timestamp);
            //Serial.println("---------------------------");
          } else {
            iteration_index += 1;
            adc_mode = 1;
          }
        } else if (temp_timestamp < ADC_TIMESTAMP) {
          // millis() must have wrapped-around. Check how long has actually elapsed since the last uc_timestamp by adding-up
          // how much was left to elapse before millis() wrapped-around, and how much has elapsed since it wrapped-around.
          unsigned long corrected_uc_timestamp = (4294967296L - ADC_TIMESTAMP) + temp_timestamp;
          if (corrected_uc_timestamp > ADC_INTEGRATION_TIME_MSECS) {
            ADC_TIMESTAMP = corrected_uc_timestamp;
          }
        }
      }
    }
  }
  return 0;
}

bool logToSD(DateTime now, int log_index) {
  bool success = false;
  char filename[13] = "            ";
  File DATA_FILE;
  // Get the current time from the RTC.
  now = RTC.now();
  // Generate current logging filename.
  sprintf(filename, "%04d%02d%02d.%03d", now.year(), now.month(), now.day(), log_index);
  DATA_FILE = SD.open(filename, O_CREAT | O_WRITE | O_APPEND);
  if (DATA_FILE) {
    success = true;
    ultoa(now.unixtime(), filename, 10);
    DATA_FILE.write(filename, 10);
    for (uint8_t measurement_index = 0; measurement_index < 8; measurement_index ++) {
      DATA_FILE.write(',');
      dtostrf(ADC_TOTALS_0[measurement_index], 9, 2, filename);
      DATA_FILE.write(filename, 9);
      DATA_FILE.write(',');
      dtostrf(ADC_TOTALS_1[measurement_index], 9, 2, filename);
      DATA_FILE.write(filename, 9);
    }
    DATA_FILE.print('\r');
    DATA_FILE.print('\n');
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

void serialSpeak(const __FlashStringHelper* characters, const int len) {
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
  }
  return 0;
}

void setTime() {
  bool message_to_process = false;
  int mode = 0;
  byte message_type = 0;
  
  message_to_process = serialListen(false);
  if ((message_to_process == true) && (CRC_SUCCESS == true)) {
    if (TIME_COMMAND_MODE == 0) {                                           // If we are waiting to receive a time header...
      message_type = 0;                                                   // Parse message against base commands.
      mode = parseMessage(message_to_process, message_type);              // 0 = No change, 1 = Set time (ST), 2 = Spool data (TX).
      if (mode != 0) {                                                    // If RPI is still sending ST command, must not have received last ack from arduino...
        COMMAND_MODE = 0;                                               // Break out of ST command mode.
        serialSpeak(F("m2"), 2);                                              // Send nack to arduino so ST command is re-sent.
      } else if (mode == 0) {                                             // If RPI is not still sending ST command, must have received last ack from arduino...
        message_type = 1;                                               // Parsing message against time headers.
        mode = parseMessage(message_to_process, message_type);          // 0 = Not recognised, 1 = Year, 2 = Month, 3 = Day, 4 = Hour, 5 = Minute, 6 = Second.
        if (mode != 0) {                                                // If time header successfully parsed...
          COMMAND_STAGE = mode - 1;
          TIME_COMMAND_MODE = 1;                                      // Switch to receiving time values instead of time headers.
          serialSpeak(F("ht"), 2);                                          // Send ack to RPI.
        } else if (mode == 0) {                                         // If time header not recognised...
          serialSpeak(F("m3"), 2);                                          // Send nack to RPI so header is re-sent.
        }
      }
    } else if (TIME_COMMAND_MODE == 1) {                                    // If we are waiting to receive a time value...
      message_type = 1;                                                   // Parse message against time headers.
      mode = parseMessage(message_to_process, message_type);              // 0 = Not recognised, 1 = Year, 2 = Month, 3 = Day, 4 = Hour, 5 = Minute, 6 = Second.
      if (mode != 0) {                                                    // If RPI is still sending time header, must not have received last ack from arduino...
        TIME_COMMAND_MODE = 0;                                          // Switch to receiving time headers instead of time values.
        serialSpeak(F("m4"), 2);                                              // Send nack to RPI.
      } else if (mode == 0) {                                             // If RPI is sending values...
        TIMESTAMP_BUFFER[COMMAND_STAGE] = atoi(INPUT_MESSAGE_BUFFER);   // Convert value from char array to int.
        TIME_COMMAND_MODE = 0;                                          // Switch back to receiving time headers.
        if (COMMAND_STAGE == 5) {                                       // If this is the last time value to get...   (assumes headers are dispatched in order!)
          COMMAND_MODE = 0;                                           // Break out of ST command mode.
          RTC.adjust(DateTime(TIMESTAMP_BUFFER[0], TIMESTAMP_BUFFER[1], TIMESTAMP_BUFFER[2], TIMESTAMP_BUFFER[3], TIMESTAMP_BUFFER[4], TIMESTAMP_BUFFER[5]));   // Set the time.
          serialSpeak(F("ts"), 2);                                          // Send final ack to RPI.
          logTelemetry(F("TUC"), 3);
        } else if (COMMAND_STAGE < 5) {                                 // If this is not the last time value to get...
          serialSpeak(F("ht"), 2);                                          // Send ack to RPI.
        }
      }
    }
    for (int j = 0; j < MAX_INPUT; j ++) {
      INPUT_MESSAGE_BUFFER[j] = '\0';
    }
    COMMAND_START_TIMESTAMP = millis();
  } else if ((message_to_process == true) && (CRC_SUCCESS == false)) {  // Whatever it was arrived garbled, send an nak to the RPI.
    serialSpeak(F("m5"), 2);                                                  // Send nack to RPI.
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
    SEND_FILE = SD.open("SEND.TXT");
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
            if (index > 8) {
              SEND_FILE_INDEX_BUFFER[index - 9] = character;
            }
            index ++;
          }
          SEND_FILE_CURSOR ++;
        }
        FILENAME_BUFFER[index] = '\0';                                      // Appends terminating \0 to buffers.
        SEND_FILE_INDEX_BUFFER[index - 9] = '\0';
        serialSpeak(SEND_FILE_INDEX_BUFFER);
        success = true;
        DATA_FILE_CURSOR = 0;                                               // Zero the data file cursor.
        SPOOL_COMMAND_MODE = 1;                                             // Switch to 'Send file index' mode.
      } else {
        success = true;
        serialSpeak(F("fs"), 2);                                                  // No more files in the SEND file list, signal to RPI we are finished.
        SEND_FILE_CURSOR = 0;                                               // Zero the SEND file cursor.
        SPOOL_COMMAND_MODE = 5;                                             // Switch to 'Completion handshake' mode.
      }
      SEND_FILE.close();
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
      if (mode == 2) {                                                          // TX received from RPI, must not have correctly received the file index string.
        serialSpeak(SEND_FILE_INDEX_BUFFER);                                      // Re-send file index buffer.
      } else {
        message_type = 2;                                                       // Parse message against TX commands.
        int new_mode = parseMessage(message_to_process, message_type);
        if (new_mode == 1) {                                                    // AK received from RPI, file index string correctly received.
          SPOOL_COMMAND_MODE = 2;                                                 // Switch to 'load data from current file' mode.
        }
      }
      COMMAND_START_TIMESTAMP = millis();
    } else if ((message_to_process == true) && (CRC_SUCCESS == false)) {        // If message from RPI fails its CRC check, we know the RPI did not receive the file index string
      serialSpeak(SEND_FILE_INDEX_BUFFER);                                      // as a garbled TX command would have been caught in the main loop. Re-send the file index buffer.
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
  } else if (SPOOL_COMMAND_MODE == 3) {
    // Send the data frame to the RPI.
    serialSpeak(OUTPUT_BUFFER);
    SPOOL_COMMAND_MODE = 4;                                                     // Switch to 'waiting for ack' mode.
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
        byte new_mode = parseMessage(message_to_process, message_type);     // 0 = Not recognised, 1 = Acknowledge frame (AK), 2 = Acknowledge resend (AR), 3 = TX completed.
        if (new_mode == 0) {                                                // CRC check passed but command not recognised, confirm resend.
          serialSpeak(F("cr"), 2);
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
      serialSpeak(F("cr"), 2);
      SPOOL_COMMAND_MODE = 4;
      COMMAND_START_TIMESTAMP = millis();
    }
  } else if (SPOOL_COMMAND_MODE == 5) {
    // Transmission complete handshaking with RPI.
    bool message_to_process = serialListen(false);
    if ((message_to_process == true) && (CRC_SUCCESS == true)) {
      byte message_type = 2;                                                  // Parse message against TX commands.
      int mode = parseMessage(message_to_process, message_type);              // 0 = Not recognised, 1 = Acknowledge frame (AK), 2 = Acknowledge resend (AR), 3 = TX completed.
      if (mode == 0) {                                                        // CRC check passed but command not recognised, confirm completion.
        serialSpeak(F("fs"), 2);
      } else if (mode == 3) {                                                 // CC received from RPI, transmission complete!
        serialSpeak(F("cc"), 2);                                                  // Send final confirmation.
        SD.remove("SEND.TXT");
        logTelemetry(F("TXC"), 3);
        COMMAND_MODE = 0;                                                   // Switch back to base command mode.
      }
      COMMAND_START_TIMESTAMP = millis();
    } else if ((message_to_process == true) && (CRC_SUCCESS == false)) {        // If message from RPI fails its CRC check, confirm completion.
      serialSpeak(F("fs"), 2);
      COMMAND_START_TIMESTAMP = millis();
    }
  }
  checkCommandTimeout();
  return success;
}

bool checkCommandTimeout() {
  bool timeout = false;
  unsigned long current_timestamp = millis();
  if (current_timestamp > (COMMAND_START_TIMESTAMP + 500L)) {
    COMMAND_MODE = 0;
    serialSpeak(F("to"), 2);
    logTelemetry(F("CTO"), 3);
    timeout = true;
  } else if (current_timestamp < COMMAND_START_TIMESTAMP) {
    // millis() must have overflowed. Correct for this.
    unsigned long corrected_timestamp = (4294967296L - COMMAND_START_TIMESTAMP) + current_timestamp;
    if (corrected_timestamp > 500L) {
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
  }
  return 0;
}

uint8_t i2cSelectChannel(uint8_t channel) {
  Wire.beginTransmission(0x70);                               // 0x70 is the address of the i2c mux.
  Wire.write(1 << channel);                                   // Switch channel.
  return Wire.endTransmission();
}

void startADCReadings(uint8_t bank) {
  for (uint8_t i = 0; i < 4; i ++) {
    i2cSelectChannel(i);
    for (uint8_t j = 0; j < ADCS_PER_CHANNEL[i]; j ++) {
      if (bank == 0) {
        ADCS[j].NB_Start_ADC_Differential_0_1();
      } else if (bank == 1) {
        ADCS[j].NB_Start_ADC_Differential_2_3();
      }
    }
  }
  return 0;
}

void accumulateADCReadings(uint8_t bank) {
  uint8_t adcs_to_this_point = 0;
  for (uint8_t i = 0; i < 4; i ++) {
    i2cSelectChannel(i);
    for (uint8_t j = 0; j < ADCS_PER_CHANNEL[i]; j ++) {
      int16_t new_measurement = ADCS[j].NB_Read_ADC_Differential();
      if (bank == 0) {
        ADC_TOTALS_0[adcs_to_this_point] += (float)new_measurement;
      } else {
        ADC_TOTALS_1[adcs_to_this_point] += (float)new_measurement;
      }
      adcs_to_this_point += 1;
    }
  }
  return 0;
}

void averageADCAccumulators() {
  for (uint8_t i = 0; i < 8; i ++) {
    ADC_TOTALS_0[i] /= (float)SAMPLES_PER_ITERATION;
    ADC_TOTALS_1[i] /= (float)SAMPLES_PER_ITERATION;
  }
  return 0;
}

void clearADCAccumulators() {
  for (uint8_t i = 0; i < 8; i ++) {
    ADC_TOTALS_0[i] = 0.0;
    ADC_TOTALS_1[i] = 0.0;
  }
  return 0;
}

