// Date and time functions using a PCF8523 RTC connected via I2C and Wire lib
#include <Wire.h>
#include <SD.h>
#include "RTClib.h"

RTC_PCF8523 RTC;

int RESET_WARNING = 0;
unsigned long UC_TIMESTAMP;
int REPEAT_COUNT = 0;
unsigned long UPDATE_RATE = 0;
unsigned long UPDATE_TIMESTAMP = 0;

// Inbound message handling.
const int NUM_COMMANDS = 2;
const int MAX_COMMAND_LENGTH = 2;
const char COMMAND_STRINGS[NUM_COMMANDS][MAX_COMMAND_LENGTH] = {"ST", "TX"};
const byte COMMAND_LENGTHS[NUM_COMMANDS] = {2, 2};
const int MAX_INPUT = 36;
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
const int OUTPUT_PACKET_LENGTH = 16;
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
  //Serial.begin(921600);
  //Serial.begin(3000000);
  delay(500);

  // Initialise Real Time Clock.
  if (! RTC.begin()) {
    Serial.println("Couldn't find RTC");
    digitalWrite(PIN_RTC_FAIL_WARN, HIGH);
    while (1);
  }

  // INITIALISE SD CARD:
  if (! SD.begin()) {
    Serial.println(F("ERROR: UNABLE TO INITIALISE SD"));
    digitalWrite(PIN_SD_FAIL_WARN, HIGH);
  }
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
    logTelemetry("AR");
  }
  return log_index;
}

bool logTelemetry(const char * characters) {
  bool success = false;
  DateTime lognow;
  File LOG_FILE;
  // Get the current time from the RTC.
  lognow = RTC.now();
  LOG_FILE = SD.open("LOG.CSV", FILE_WRITE);
  if (LOG_FILE) {
    success = true;
    LOG_FILE.print(lognow.unixtime());
    LOG_FILE.print(",");
    for (int n = 0; *characters != '\0'; characters ++) {
      LOG_FILE.print(*characters);
    }
    LOG_FILE.print("\r\n");
    LOG_FILE.close();
  }
  return success;
}

bool logFileToSend(const char * characters) {
  bool success = false;
  File SEND_FILE;
  SEND_FILE = SD.open("SEND.TXT", FILE_WRITE);
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
  unsigned long temp_timestamp;
  int log_index = getLogIndex();
  char filename[13] = "            ";

  while (true) {
    // Listen for any new incoming characters over serial connection, set new mode appropriately.
    bool message_to_process;
    int mode;
    byte message_type;

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
              serialSpeak("ak");
              COMMAND_MODE = 1;
              TIME_COMMAND_MODE = 0;
              logTelemetry("TUS");
            }
            if (mode == 2) {
              now = RTC.now();
              sprintf(filename, "%04d%02d%02d.%03d", now.year(), now.month(), now.day(), log_index);              // These lines add the current file to the 'to-send' list
              logFileToSend(filename);                                                                            // Then increase the index so the next data log will create
              log_index ++;                                                                                       // a new logging file (todays date, index incremented).
              COMMAND_MODE = 2;
              SPOOL_COMMAND_MODE = 0;
              logTelemetry("TXS");
            }
            COMMAND_START_TIMESTAMP = millis();
          }
        } else if (CRC_SUCCESS == false) {
          serialSpeak("fl");
          COMMAND_MODE = 0;
        }
        for (int j = 0; j < MAX_INPUT; j ++) {
          INPUT_MESSAGE_BUFFER[j] = '0';
        }
      }
    } else {
      applyCommand(COMMAND_MODE);
    }

    // Get the current timestamp.
    temp_timestamp = millis();

    // If 125msec or more has elapsed since the last UC_TIMESTAMP, read the analog sensors.
    if (temp_timestamp > (UC_TIMESTAMP + 125L)) {
      UC_TIMESTAMP = temp_timestamp;
      readSensors();
    } else if (temp_timestamp < UC_TIMESTAMP) {
      // millis() must have wrapped-around. Check how long has actually elapsed since the last uc_timestamp by adding-up
      // how much was left to elapse before millis() wrapped-around, and how much has elapsed since it wrapped-around.
      unsigned long corrected_uc_timestamp = (4294967296L - UC_TIMESTAMP) + temp_timestamp;
      if (corrected_uc_timestamp > 125L) {
        UC_TIMESTAMP = corrected_uc_timestamp;
      }
    }

    // If we have read the sensors 8 times (at 8Hz), one second has elapsed and it's time to update the rasperry pi.
    if (REPEAT_COUNT > 7) {
      REPEAT_COUNT = 0;
      DateTime pre_update_now = now;
      now = RTC.now();
      if (now.day() != pre_update_now.day()) {        // If the day has changed since we last wrote measurements to the SD card, we've rolled-over Midnight, or changed the date,
        // so we start a new extension index and add the last filename to the 'to send' file.
        sprintf(filename, "%04d%02d%02d.%03d", pre_update_now.year(), pre_update_now.month(), pre_update_now.day(), log_index);
        logFileToSend(filename);
        log_index = 0;
      }
      logToSD(now, log_index);
    }
  }
}

void readSensors() {
  REPEAT_COUNT ++;
}

bool logToSD(DateTime now, int log_index) {
  bool success = false;
  char filename[13] = "            ";
  File DATA_FILE;
  // Get the current time from the RTC.
  now = RTC.now();
  // Generate current logging filename.
  sprintf(filename, "%04d%02d%02d.%03d", now.year(), now.month(), now.day(), log_index);
  DATA_FILE = SD.open(filename, FILE_WRITE);
  if (DATA_FILE) {
    success = true;
    DATA_FILE.print(now.unixtime());
    DATA_FILE.print("\r\n");
    DATA_FILE.close();
  }
  return success;
}

bool serialListen(bool blocking) {
  // Add incoming characters in hardware serial buffer to message buffer while available. Block until terminating \n
  // character reached if blocking = true. If terminating character received zero message buffer index and return true.
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
        serialSpeak("m2");                                              // Send nack to arduino so ST command is re-sent.
      } else if (mode == 0) {                                             // If RPI is not still sending ST command, must have received last ack from arduino...
        message_type = 1;                                               // Parsing message against time headers.
        mode = parseMessage(message_to_process, message_type);          // 0 = Not recognised, 1 = Year, 2 = Month, 3 = Day, 4 = Hour, 5 = Minute, 6 = Second.
        if (mode != 0) {                                                // If time header successfully parsed...
          COMMAND_STAGE = mode - 1;
          TIME_COMMAND_MODE = 1;                                      // Switch to receiving time values instead of time headers.
          serialSpeak("ht");                                          // Send ack to RPI.
        } else if (mode == 0) {                                         // If time header not recognised...
          serialSpeak("m3");                                          // Send nack to RPI so header is re-sent.
        }
      }
    } else if (TIME_COMMAND_MODE == 1) {                                    // If we are waiting to receive a time value...
      message_type = 1;                                                   // Parse message against time headers.
      mode = parseMessage(message_to_process, message_type);              // 0 = Not recognised, 1 = Year, 2 = Month, 3 = Day, 4 = Hour, 5 = Minute, 6 = Second.
      if (mode != 0) {                                                    // If RPI is still sending time header, must not have received last ack from arduino...
        TIME_COMMAND_MODE = 0;                                          // Switch to receiving time headers instead of time values.
        serialSpeak("m4");                                              // Send nack to RPI.
      } else if (mode == 0) {                                             // If RPI is sending values...
        TIMESTAMP_BUFFER[COMMAND_STAGE] = atoi(INPUT_MESSAGE_BUFFER);   // Convert value from char array to int.
        TIME_COMMAND_MODE = 0;                                          // Switch back to receiving time headers.
        if (COMMAND_STAGE == 5) {                                       // If this is the last time value to get...   (assumes headers are dispatched in order!)
          COMMAND_MODE = 0;                                           // Break out of ST command mode.
          RTC.adjust(DateTime(TIMESTAMP_BUFFER[0], TIMESTAMP_BUFFER[1], TIMESTAMP_BUFFER[2], TIMESTAMP_BUFFER[3], TIMESTAMP_BUFFER[4], TIMESTAMP_BUFFER[5]));   // Set the time.
          serialSpeak("ts");                                          // Send final ack to RPI.
          logTelemetry("TUC");
        } else if (COMMAND_STAGE < 5) {                                 // If this is not the last time value to get...
          serialSpeak("ht");                                          // Send ack to RPI.
        }
      }
    }
    for (int j = 0; j < MAX_INPUT; j ++) {
      INPUT_MESSAGE_BUFFER[j] = '\0';
    }
    COMMAND_START_TIMESTAMP = millis();
  } else if ((message_to_process == true) && (CRC_SUCCESS == false)) {  // Whatever it was arrived garbled, send an nak to the RPI.
    serialSpeak("m5");                                                  // Send nack to RPI.
    for (int j = 0; j < MAX_INPUT; j ++) {
      INPUT_MESSAGE_BUFFER[j] = '\0';
    }
    COMMAND_START_TIMESTAMP = millis();
  }
  checkCommandTimeout();
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
        serialSpeak("fs");                                                  // No more files in the SEND file list, signal to RPI we are finished.
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
      if (DATA_FILE_CURSOR < data_file_size) {                                // If we haven't reached the end of the DATA file...
        DATA_FILE.seek(DATA_FILE_CURSOR);
        for (int i = 0; i < OUTPUT_PACKET_LENGTH; i ++) {                                      // Reads next 8 bytes from DATA file into buffer.
          if (DATA_FILE_CURSOR < data_file_size) {                        // Check we dont go past the end of the DATA file...
            char character = DATA_FILE.read();
            OUTPUT_BUFFER[i] = character;                               // Store the filename in a global buffer.
            DATA_FILE_CURSOR ++;
          } else {
            OUTPUT_BUFFER[i] = '\0';                                    // If this read is beyond the end of the DATA file, pad the output buffer with \0 characters.
            EOF_FLAG = true;                                            // Set the end-of-file flag.
          }
        }
        OUTPUT_BUFFER[OUTPUT_PACKET_LENGTH] = '\0';                                            // Appends terminating \0 to buffer.
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
          serialSpeak("cr");
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
      serialSpeak("cr");
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
        serialSpeak("fs");
      } else if (mode == 3) {                                                 // CC received from RPI, transmission complete!
        serialSpeak("cc");                                                  // Send final confirmation.
        SD.remove("SEND.TXT");
        logTelemetry("TXC");
        COMMAND_MODE = 0;                                                   // Switch back to base command mode.
      }
      COMMAND_START_TIMESTAMP = millis();
    } else if ((message_to_process == true) && (CRC_SUCCESS == false)) {        // If message from RPI fails its CRC check, confirm completion.
      serialSpeak("fs");
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
    serialSpeak("to");
    logTelemetry("CTO");
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
