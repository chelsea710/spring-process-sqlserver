package com.zenith.springprocesssqlserver.pojo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.HashMap;

public class CsvReader {
    private Reader inputStream = null;

    private String fileName = null;

    private UserSettings userSettings = new UserSettings();

    private Charset charset = null;

    private boolean useCustomRecordDelimiter = false;

    private DataBuffer dataBuffer = new DataBuffer();

    private ColumnBuffer columnBuffer = new ColumnBuffer();

    private RawRecordBuffer rawBuffer = new RawRecordBuffer();

    private boolean[] isQualified = null;

    private String rawRecord = "";

    private HeadersHolder headersHolder = new HeadersHolder();

    private boolean startedColumn = false;

    private boolean startedWithQualifier = false;

    private boolean hasMoreData = true;

    private char lastLetter = Character.MIN_VALUE;

    private boolean hasReadNextLine = false;

    private int columnsCount = 0;

    private long currentRecord = 0L;

    private String[] values = new String[10];

    private boolean initialized = false;

    private boolean closed = false;

    public static final int ESCAPE_MODE_DOUBLED = 1;

    public static final int ESCAPE_MODE_BACKSLASH = 2;

    public CsvReader(String fileName, char delimiter, Charset charset) throws FileNotFoundException {
        if (fileName == null)
            throw new IllegalArgumentException("Parameter fileName can not be null.");
        if (charset == null)
            throw new IllegalArgumentException("Parameter charset can not be null.");
        if (!(new File(fileName)).exists())
            throw new FileNotFoundException("File " + fileName + " does not exist.");
        this.fileName = fileName;
        this.userSettings.Delimiter = delimiter;
        this.charset = charset;
        this.isQualified = new boolean[this.values.length];
    }

    public CsvReader(String fileName, char delimiter) throws FileNotFoundException {
        this(fileName, delimiter, Charset.forName("ISO-8859-1"));
    }

    public CsvReader(String fileName) throws FileNotFoundException {
        this(fileName, ',');
    }

    public CsvReader(Reader inputStream, char delimiter) {
        if (inputStream == null)
            throw new IllegalArgumentException("Parameter inputStream can not be null.");
        this.inputStream = inputStream;
        this.userSettings.Delimiter = delimiter;
        this.initialized = true;
        this.isQualified = new boolean[this.values.length];
    }

    public CsvReader(Reader inputStream) {
        this(inputStream, ',');
    }

    public CsvReader(InputStream inputStream, char delimiter, Charset charset) {
        this(new InputStreamReader(inputStream, charset), delimiter);
    }

    public CsvReader(InputStream inputStream, Charset charset) {
        this(new InputStreamReader(inputStream, charset));
    }

    public boolean getCaptureRawRecord() {
        return this.userSettings.CaptureRawRecord;
    }

    public void setCaptureRawRecord(boolean captureRawRecord) {
        this.userSettings.CaptureRawRecord = captureRawRecord;
    }

    public String getRawRecord() {
        return this.rawRecord;
    }

    public boolean getTrimWhitespace() {
        return this.userSettings.TrimWhitespace;
    }

    public void setTrimWhitespace(boolean trimWhitespace) {
        this.userSettings.TrimWhitespace = trimWhitespace;
    }

    public char getDelimiter() {
        return this.userSettings.Delimiter;
    }

    public void setDelimiter(char delimiter) {
        this.userSettings.Delimiter = delimiter;
    }

    public char getRecordDelimiter() {
        return this.userSettings.RecordDelimiter;
    }

    public void setRecordDelimiter(char recordDelimiter) {
        this.useCustomRecordDelimiter = true;
        this.userSettings.RecordDelimiter = recordDelimiter;
    }

    public char getTextQualifier() {
        return this.userSettings.TextQualifier;
    }

    public void setTextQualifier(char textQualifier) {
        this.userSettings.TextQualifier = textQualifier;
    }

    public boolean getUseTextQualifier() {
        return this.userSettings.UseTextQualifier;
    }

    public void setUseTextQualifier(boolean useTextQualifier) {
        this.userSettings.UseTextQualifier = useTextQualifier;
    }

    public char getComment() {
        return this.userSettings.Comment;
    }

    public void setComment(char comment) {
        this.userSettings.Comment = comment;
    }

    public boolean getUseComments() {
        return this.userSettings.UseComments;
    }

    public void setUseComments(boolean useComments) {
        this.userSettings.UseComments = useComments;
    }

    public int getEscapeMode() {
        return this.userSettings.EscapeMode;
    }

    public void setEscapeMode(int escapeMode) throws IllegalArgumentException {
        if (escapeMode != 1 && escapeMode != 2)
            throw new IllegalArgumentException("Parameter escapeMode must be a valid value.");
        this.userSettings.EscapeMode = escapeMode;
    }

    public boolean getSkipEmptyRecords() {
        return this.userSettings.SkipEmptyRecords;
    }

    public void setSkipEmptyRecords(boolean skipEmptyRecords) {
        this.userSettings.SkipEmptyRecords = skipEmptyRecords;
    }

    public boolean getSafetySwitch() {
        return this.userSettings.SafetySwitch;
    }

    public void setSafetySwitch(boolean safetySwitch) {
        this.userSettings.SafetySwitch = safetySwitch;
    }

    public int getColumnCount() {
        return this.columnsCount;
    }

    public long getCurrentRecord() {
        return this.currentRecord - 1L;
    }

    public int getHeaderCount() {
        return this.headersHolder.Length;
    }

    public String[] getHeaders() throws IOException {
        checkClosed();
        if (this.headersHolder.Headers == null)
            return null;
        String[] clone = new String[this.headersHolder.Length];
        System.arraycopy(this.headersHolder.Headers, 0, clone, 0, this.headersHolder.Length);
        return clone;
    }

    public void setHeaders(String[] headers) {
        this.headersHolder.Headers = headers;
        this.headersHolder.IndexByName.clear();
        if (headers != null) {
            this.headersHolder.Length = headers.length;
        } else {
            this.headersHolder.Length = 0;
        }
        for (int i = 0; i < this.headersHolder.Length; i++)
            this.headersHolder.IndexByName.put(headers[i], new Integer(i));
    }

    public String[] getValues() throws IOException {
        checkClosed();
        String[] clone = new String[this.columnsCount];
        System.arraycopy(this.values, 0, clone, 0, this.columnsCount);
        return clone;
    }

    public String get(int columnIndex) throws IOException {
        checkClosed();
        if (columnIndex > -1 && columnIndex < this.columnsCount)
            return this.values[columnIndex];
        return "";
    }

    public String get(String headerName) throws IOException {
        checkClosed();
        return get(getIndex(headerName));
    }

    public static CsvReader parse(String data) {
        if (data == null)
            throw new IllegalArgumentException("Parameter data can not be null.");
        return new CsvReader(new StringReader(data));
    }

    public boolean readRecord() throws IOException {
        checkClosed();
        this.columnsCount = 0;
        this.rawBuffer.Position = 0;
        this.dataBuffer.LineStart = this.dataBuffer.Position;
        this.hasReadNextLine = false;
        if (this.hasMoreData) {
            do {
                if (this.dataBuffer.Position == this.dataBuffer.Count) {
                    checkDataLength();
                } else {
                    this.startedWithQualifier = false;
                    char currentLetter = this.dataBuffer.Buffer[this.dataBuffer.Position];
                    if (this.userSettings.UseTextQualifier && currentLetter == this.userSettings.TextQualifier) {
                        this.lastLetter = currentLetter;
                        this.startedColumn = true;
                        this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
                        this.startedWithQualifier = true;
                        boolean lastLetterWasQualifier = false;
                        char escapeChar = this.userSettings.TextQualifier;
                        if (this.userSettings.EscapeMode == 2)
                            escapeChar = '\\';
                        boolean eatingTrailingJunk = false;
                        boolean lastLetterWasEscape = false;
                        boolean readingComplexEscape = false;
                        int escape = 1;
                        int escapeLength = 0;
                        char escapeValue = Character.MIN_VALUE;
                        this.dataBuffer.Position++;
                        do {
                            if (this.dataBuffer.Position == this.dataBuffer.Count) {
                                checkDataLength();
                            } else {
                                currentLetter = this.dataBuffer.Buffer[this.dataBuffer.Position];
                                if (eatingTrailingJunk) {
                                    this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
                                    if (currentLetter == this.userSettings.Delimiter) {
                                        endColumn();
                                    } else if ((!this.useCustomRecordDelimiter && (currentLetter == '\r' || currentLetter == '\n')) || (this.useCustomRecordDelimiter && currentLetter == this.userSettings.RecordDelimiter)) {
                                        endColumn();
                                        endRecord();
                                    }
                                } else if (readingComplexEscape) {
                                    escapeLength++;
                                    switch (escape) {
                                        case 1:
                                            escapeValue = (char)(escapeValue * 16);
                                            escapeValue = (char)(escapeValue + hexToDec(currentLetter));
                                            if (escapeLength == 4)
                                                readingComplexEscape = false;
                                            break;
                                        case 2:
                                            escapeValue = (char)(escapeValue * 8);
                                            escapeValue = (char)(escapeValue + (char)(currentLetter - 48));
                                            if (escapeLength == 3)
                                                readingComplexEscape = false;
                                            break;
                                        case 3:
                                            escapeValue = (char)(escapeValue * 10);
                                            escapeValue = (char)(escapeValue + (char)(currentLetter - 48));
                                            if (escapeLength == 3)
                                                readingComplexEscape = false;
                                            break;
                                        case 4:
                                            escapeValue = (char)(escapeValue * 16);
                                            escapeValue = (char)(escapeValue + hexToDec(currentLetter));
                                            if (escapeLength == 2)
                                                readingComplexEscape = false;
                                            break;
                                    }
                                    if (!readingComplexEscape) {
                                        appendLetter(escapeValue);
                                    } else {
                                        this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
                                    }
                                } else if (currentLetter == this.userSettings.TextQualifier) {
                                    if (lastLetterWasEscape) {
                                        lastLetterWasEscape = false;
                                        lastLetterWasQualifier = false;
                                    } else {
                                        updateCurrentValue();
                                        if (this.userSettings.EscapeMode == 1)
                                            lastLetterWasEscape = true;
                                        lastLetterWasQualifier = true;
                                    }
                                } else if (this.userSettings.EscapeMode == 2 && lastLetterWasEscape) {
                                    switch (currentLetter) {
                                        case 'n':
                                            appendLetter('\n');
                                            break;
                                        case 'r':
                                            appendLetter('\r');
                                            break;
                                        case 't':
                                            appendLetter('\t');
                                            break;
                                        case 'b':
                                            appendLetter('\b');
                                            break;
                                        case 'f':
                                            appendLetter('\f');
                                            break;
                                        case 'e':
                                            appendLetter('\033');
                                            break;
                                        case 'v':
                                            appendLetter('\013');
                                            break;
                                        case 'a':
                                            appendLetter('\007');
                                            break;
                                        case '0':
                                        case '1':
                                        case '2':
                                        case '3':
                                        case '4':
                                        case '5':
                                        case '6':
                                        case '7':
                                            escape = 2;
                                            readingComplexEscape = true;
                                            escapeLength = 1;
                                            escapeValue = (char)(currentLetter - 48);
                                            this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
                                            break;
                                        case 'D':
                                        case 'O':
                                        case 'U':
                                        case 'X':
                                        case 'd':
                                        case 'o':
                                        case 'u':
                                        case 'x':
                                            switch (currentLetter) {
                                                case 'U':
                                                case 'u':
                                                    escape = 1;
                                                    break;
                                                case 'X':
                                                case 'x':
                                                    escape = 4;
                                                    break;
                                                case 'O':
                                                case 'o':
                                                    escape = 2;
                                                    break;
                                                case 'D':
                                                case 'd':
                                                    escape = 3;
                                                    break;
                                            }
                                            readingComplexEscape = true;
                                            escapeLength = 0;
                                            escapeValue = Character.MIN_VALUE;
                                            this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
                                            break;
                                    }
                                    lastLetterWasEscape = false;
                                } else if (currentLetter == escapeChar) {
                                    updateCurrentValue();
                                    lastLetterWasEscape = true;
                                } else if (lastLetterWasQualifier) {
                                    if (currentLetter == this.userSettings.Delimiter) {
                                        endColumn();
                                    } else if ((!this.useCustomRecordDelimiter && (currentLetter == '\r' || currentLetter == '\n')) || (this.useCustomRecordDelimiter && currentLetter == this.userSettings.RecordDelimiter)) {
                                        endColumn();
                                        endRecord();
                                    } else {
                                        this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
                                        eatingTrailingJunk = true;
                                    }
                                    lastLetterWasQualifier = false;
                                }
                                this.lastLetter = currentLetter;
                                if (this.startedColumn) {
                                    this.dataBuffer.Position++;
                                    if (this.userSettings.SafetySwitch && this.dataBuffer.Position - this.dataBuffer.ColumnStart + this.columnBuffer.Position > 100000) {
                                        close();
                                        throw new IOException("Maximum column length of 100,000 exceeded in column " +

                                                NumberFormat.getIntegerInstance()
                                                        .format(this.columnsCount) + " in record " +

                                                NumberFormat.getIntegerInstance()
                                                        .format(this.currentRecord) + ". Set the SafetySwitch property to false if you're expecting column lengths greater than 100,000 characters to avoid this error.");
                                    }
                                }
                            }
                        } while (this.hasMoreData && this.startedColumn);
                    } else if (currentLetter == this.userSettings.Delimiter) {
                        this.lastLetter = currentLetter;
                        endColumn();
                    } else if (this.useCustomRecordDelimiter && currentLetter == this.userSettings.RecordDelimiter) {
                        if (this.startedColumn || this.columnsCount > 0 || !this.userSettings.SkipEmptyRecords) {
                            endColumn();
                            endRecord();
                        } else {
                            this.dataBuffer.LineStart = this.dataBuffer.Position + 1;
                        }
                        this.lastLetter = currentLetter;
                    } else if (!this.useCustomRecordDelimiter && (currentLetter == '\r' || currentLetter == '\n')) {
                        if (this.startedColumn || this.columnsCount > 0 || (!this.userSettings.SkipEmptyRecords && (currentLetter == '\r' || this.lastLetter != '\r'))) {
                            endColumn();
                            endRecord();
                        } else {
                            this.dataBuffer.LineStart = this.dataBuffer.Position + 1;
                        }
                        this.lastLetter = currentLetter;
                    } else if (this.userSettings.UseComments && this.columnsCount == 0 && currentLetter == this.userSettings.Comment) {
                        this.lastLetter = currentLetter;
                        skipLine();
                    } else if (this.userSettings.TrimWhitespace && (currentLetter == ' ' || currentLetter == '\t')) {
                        this.startedColumn = true;
                        this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
                    } else {
                        this.startedColumn = true;
                        this.dataBuffer.ColumnStart = this.dataBuffer.Position;
                        boolean lastLetterWasBackslash = false;
                        boolean readingComplexEscape = false;
                        int escape = 1;
                        int escapeLength = 0;
                        char escapeValue = Character.MIN_VALUE;
                        boolean firstLoop = true;
                        do {
                            if (!firstLoop && this.dataBuffer.Position == this.dataBuffer.Count) {
                                checkDataLength();
                            } else {
                                if (!firstLoop)
                                    currentLetter = this.dataBuffer.Buffer[this.dataBuffer.Position];
                                if (!this.userSettings.UseTextQualifier && this.userSettings.EscapeMode == 2 && currentLetter == '\\') {
                                    if (lastLetterWasBackslash) {
                                        lastLetterWasBackslash = false;
                                    } else {
                                        updateCurrentValue();
                                        lastLetterWasBackslash = true;
                                    }
                                } else if (readingComplexEscape) {
                                    escapeLength++;
                                    switch (escape) {
                                        case 1:
                                            escapeValue = (char)(escapeValue * 16);
                                            escapeValue = (char)(escapeValue + hexToDec(currentLetter));
                                            if (escapeLength == 4)
                                                readingComplexEscape = false;
                                            break;
                                        case 2:
                                            escapeValue = (char)(escapeValue * 8);
                                            escapeValue = (char)(escapeValue + (char)(currentLetter - 48));
                                            if (escapeLength == 3)
                                                readingComplexEscape = false;
                                            break;
                                        case 3:
                                            escapeValue = (char)(escapeValue * 10);
                                            escapeValue = (char)(escapeValue + (char)(currentLetter - 48));
                                            if (escapeLength == 3)
                                                readingComplexEscape = false;
                                            break;
                                        case 4:
                                            escapeValue = (char)(escapeValue * 16);
                                            escapeValue = (char)(escapeValue + hexToDec(currentLetter));
                                            if (escapeLength == 2)
                                                readingComplexEscape = false;
                                            break;
                                    }
                                    if (!readingComplexEscape) {
                                        appendLetter(escapeValue);
                                    } else {
                                        this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
                                    }
                                } else if (this.userSettings.EscapeMode == 2 && lastLetterWasBackslash) {
                                    switch (currentLetter) {
                                        case 'n':
                                            appendLetter('\n');
                                            break;
                                        case 'r':
                                            appendLetter('\r');
                                            break;
                                        case 't':
                                            appendLetter('\t');
                                            break;
                                        case 'b':
                                            appendLetter('\b');
                                            break;
                                        case 'f':
                                            appendLetter('\f');
                                            break;
                                        case 'e':
                                            appendLetter('\033');
                                            break;
                                        case 'v':
                                            appendLetter('\013');
                                            break;
                                        case 'a':
                                            appendLetter('\007');
                                            break;
                                        case '0':
                                        case '1':
                                        case '2':
                                        case '3':
                                        case '4':
                                        case '5':
                                        case '6':
                                        case '7':
                                            escape = 2;
                                            readingComplexEscape = true;
                                            escapeLength = 1;
                                            escapeValue = (char)(currentLetter - 48);
                                            this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
                                            break;
                                        case 'D':
                                        case 'O':
                                        case 'U':
                                        case 'X':
                                        case 'd':
                                        case 'o':
                                        case 'u':
                                        case 'x':
                                            switch (currentLetter) {
                                                case 'U':
                                                case 'u':
                                                    escape = 1;
                                                    break;
                                                case 'X':
                                                case 'x':
                                                    escape = 4;
                                                    break;
                                                case 'O':
                                                case 'o':
                                                    escape = 2;
                                                    break;
                                                case 'D':
                                                case 'd':
                                                    escape = 3;
                                                    break;
                                            }
                                            readingComplexEscape = true;
                                            escapeLength = 0;
                                            escapeValue = Character.MIN_VALUE;
                                            this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
                                            break;
                                    }
                                    lastLetterWasBackslash = false;
                                } else if (currentLetter == this.userSettings.Delimiter) {
                                    endColumn();
                                } else if ((!this.useCustomRecordDelimiter && (currentLetter == '\r' || currentLetter == '\n')) || (this.useCustomRecordDelimiter && currentLetter == this.userSettings.RecordDelimiter)) {
                                    endColumn();
                                    endRecord();
                                }
                                this.lastLetter = currentLetter;
                                firstLoop = false;
                                if (this.startedColumn) {
                                    this.dataBuffer.Position++;
                                    if (this.userSettings.SafetySwitch && this.dataBuffer.Position - this.dataBuffer.ColumnStart + this.columnBuffer.Position > 100000) {
                                        close();
                                        throw new IOException("Maximum column length of 100,000 exceeded in column " +

                                                NumberFormat.getIntegerInstance()
                                                        .format(this.columnsCount) + " in record " +

                                                NumberFormat.getIntegerInstance()
                                                        .format(this.currentRecord) + ". Set the SafetySwitch property to false if you're expecting column lengths greater than 100,000 characters to avoid this error.");
                                    }
                                }
                            }
                        } while (this.hasMoreData && this.startedColumn);
                    }
                    if (this.hasMoreData)
                        this.dataBuffer.Position++;
                }
            } while (this.hasMoreData && !this.hasReadNextLine);
            if (this.startedColumn || this.lastLetter == this.userSettings.Delimiter) {
                endColumn();
                endRecord();
            }
        }
        if (this.userSettings.CaptureRawRecord) {
            if (this.hasMoreData) {
                if (this.rawBuffer.Position == 0) {
                    this.rawRecord = new String(this.dataBuffer.Buffer, this.dataBuffer.LineStart, this.dataBuffer.Position - this.dataBuffer.LineStart - 1);
                } else {
                    this.rawRecord = new String(this.rawBuffer.Buffer, 0, this.rawBuffer.Position) + new String(this.dataBuffer.Buffer, this.dataBuffer.LineStart, this.dataBuffer.Position - this.dataBuffer.LineStart - 1);
                }
            } else {
                this.rawRecord = new String(this.rawBuffer.Buffer, 0, this.rawBuffer.Position);
            }
        } else {
            this.rawRecord = "";
        }
        return this.hasReadNextLine;
    }

    private void checkDataLength() throws IOException {
        if (!this.initialized) {
            if (this.fileName != null)
                this.inputStream = new BufferedReader(new InputStreamReader(new FileInputStream(this.fileName), this.charset), 4096);
            this.charset = null;
            this.initialized = true;
        }
        updateCurrentValue();
        if (this.userSettings.CaptureRawRecord && this.dataBuffer.Count > 0) {
            if (this.rawBuffer.Buffer.length - this.rawBuffer.Position < this.dataBuffer.Count - this.dataBuffer.LineStart) {
                int newLength = this.rawBuffer.Buffer.length + Math.max(this.dataBuffer.Count - this.dataBuffer.LineStart, this.rawBuffer.Buffer.length);
                char[] holder = new char[newLength];
                System.arraycopy(this.rawBuffer.Buffer, 0, holder, 0, this.rawBuffer.Position);
                this.rawBuffer.Buffer = holder;
            }
            System.arraycopy(this.dataBuffer.Buffer, this.dataBuffer.LineStart, this.rawBuffer.Buffer, this.rawBuffer.Position, this.dataBuffer.Count - this.dataBuffer.LineStart);
            this.rawBuffer.Position += this.dataBuffer.Count - this.dataBuffer.LineStart;
        }
        try {
            this.dataBuffer.Count = this.inputStream.read(this.dataBuffer.Buffer, 0, this.dataBuffer.Buffer.length);
        } catch (IOException ex) {
            close();
            throw ex;
        }
        if (this.dataBuffer.Count == -1)
            this.hasMoreData = false;
        this.dataBuffer.Position = 0;
        this.dataBuffer.LineStart = 0;
        this.dataBuffer.ColumnStart = 0;
    }

    public boolean readHeaders() throws IOException {
        boolean result = readRecord();
        this.headersHolder.Length = this.columnsCount;
        this.headersHolder.Headers = new String[this.columnsCount];
        for (int i = 0; i < this.headersHolder.Length; i++) {
            String columnValue = get(i);
            this.headersHolder.Headers[i] = columnValue;
            this.headersHolder.IndexByName.put(columnValue, new Integer(i));
        }
        if (result)
            this.currentRecord--;
        this.columnsCount = 0;
        return result;
    }

    public String getHeader(int columnIndex) throws IOException {
        checkClosed();
        if (columnIndex > -1 && columnIndex < this.headersHolder.Length)
            return this.headersHolder.Headers[columnIndex];
        return "";
    }

    public boolean isQualified(int columnIndex) throws IOException {
        checkClosed();
        if (columnIndex < this.columnsCount && columnIndex > -1)
            return this.isQualified[columnIndex];
        return false;
    }

    private void endColumn() throws IOException {
        String currentValue = "";
        if (this.startedColumn)
            if (this.columnBuffer.Position == 0) {
                if (this.dataBuffer.ColumnStart < this.dataBuffer.Position) {
                    int lastLetter = this.dataBuffer.Position - 1;
                    if (this.userSettings.TrimWhitespace && !this.startedWithQualifier)
                        while (lastLetter >= this.dataBuffer.ColumnStart && (this.dataBuffer.Buffer[lastLetter] == ' ' || this.dataBuffer.Buffer[lastLetter] == '\t'))
                            lastLetter--;
                    currentValue = new String(this.dataBuffer.Buffer, this.dataBuffer.ColumnStart, lastLetter - this.dataBuffer.ColumnStart + 1);
                }
            } else {
                updateCurrentValue();
                int lastLetter = this.columnBuffer.Position - 1;
                if (this.userSettings.TrimWhitespace && !this.startedWithQualifier)
                    while (lastLetter >= 0 && (this.columnBuffer.Buffer[lastLetter] == ' ' || this.columnBuffer.Buffer[lastLetter] == ' '))
                        lastLetter--;
                currentValue = new String(this.columnBuffer.Buffer, 0, lastLetter + 1);
            }
        this.columnBuffer.Position = 0;
        this.startedColumn = false;
        if (this.columnsCount >= 100000 && this.userSettings.SafetySwitch) {
            close();
            throw new IOException("Maximum column count of 100,000 exceeded in record " +

                    NumberFormat.getIntegerInstance().format(this.currentRecord) + ". Set the SafetySwitch property to false if you're expecting more than 100,000 columns per record to avoid this error.");
        }
        if (this.columnsCount == this.values.length) {
            int newLength = this.values.length * 2;
            String[] holder = new String[newLength];
            System.arraycopy(this.values, 0, holder, 0, this.values.length);
            this.values = holder;
            boolean[] qualifiedHolder = new boolean[newLength];
            System.arraycopy(this.isQualified, 0, qualifiedHolder, 0, this.isQualified.length);
            this.isQualified = qualifiedHolder;
        }
        this.values[this.columnsCount] = currentValue;
        this.isQualified[this.columnsCount] = this.startedWithQualifier;
        currentValue = "";
        this.columnsCount++;
    }

    private void appendLetter(char letter) {
        if (this.columnBuffer.Position == this.columnBuffer.Buffer.length) {
            int newLength = this.columnBuffer.Buffer.length * 2;
            char[] holder = new char[newLength];
            System.arraycopy(this.columnBuffer.Buffer, 0, holder, 0, this.columnBuffer.Position);
            this.columnBuffer.Buffer = holder;
        }
        this.columnBuffer.Buffer[this.columnBuffer.Position++] = letter;
        this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
    }

    private void updateCurrentValue() {
        if (this.startedColumn && this.dataBuffer.ColumnStart < this.dataBuffer.Position) {
            if (this.columnBuffer.Buffer.length - this.columnBuffer.Position < this.dataBuffer.Position - this.dataBuffer.ColumnStart) {
                int newLength = this.columnBuffer.Buffer.length + Math.max(this.dataBuffer.Position - this.dataBuffer.ColumnStart, this.columnBuffer.Buffer.length);
                char[] holder = new char[newLength];
                System.arraycopy(this.columnBuffer.Buffer, 0, holder, 0, this.columnBuffer.Position);
                this.columnBuffer.Buffer = holder;
            }
            System.arraycopy(this.dataBuffer.Buffer, this.dataBuffer.ColumnStart, this.columnBuffer.Buffer, this.columnBuffer.Position, this.dataBuffer.Position - this.dataBuffer.ColumnStart);
            this.columnBuffer.Position += this.dataBuffer.Position - this.dataBuffer.ColumnStart;
        }
        this.dataBuffer.ColumnStart = this.dataBuffer.Position + 1;
    }

    private void endRecord() throws IOException {
        this.hasReadNextLine = true;
        this.currentRecord++;
    }

    public int getIndex(String headerName) throws IOException {
        checkClosed();
        Object indexValue = this.headersHolder.IndexByName.get(headerName);
        if (indexValue != null)
            return ((Integer)indexValue).intValue();
        return -1;
    }

    public boolean skipRecord() throws IOException {
        checkClosed();
        boolean recordRead = false;
        if (this.hasMoreData) {
            recordRead = readRecord();
            if (recordRead)
                this.currentRecord--;
        }
        return recordRead;
    }

    public boolean skipLine() throws IOException {
        checkClosed();
        this.columnsCount = 0;
        boolean skippedLine = false;
        if (this.hasMoreData) {
            boolean foundEol = false;
            do {
                if (this.dataBuffer.Position == this.dataBuffer.Count) {
                    checkDataLength();
                } else {
                    skippedLine = true;
                    char currentLetter = this.dataBuffer.Buffer[this.dataBuffer.Position];
                    if (currentLetter == '\r' || currentLetter == '\n')
                        foundEol = true;
                    this.lastLetter = currentLetter;
                    if (!foundEol)
                        this.dataBuffer.Position++;
                }
            } while (this.hasMoreData && !foundEol);
            this.columnBuffer.Position = 0;
            this.dataBuffer.LineStart = this.dataBuffer.Position + 1;
        }
        this.rawBuffer.Position = 0;
        this.rawRecord = "";
        return skippedLine;
    }

    public void close() {
        if (!this.closed) {
            close(true);
            this.closed = true;
        }
    }

    private void close(boolean closing) {
        if (!this.closed) {
            if (closing) {
                this.charset = null;
                this.headersHolder.Headers = null;
                this.headersHolder.IndexByName = null;
                this.dataBuffer.Buffer = null;
                this.columnBuffer.Buffer = null;
                this.rawBuffer.Buffer = null;
            }
            try {
                if (this.initialized)
                    this.inputStream.close();
            } catch (Exception exception) {}
            this.inputStream = null;
            this.closed = true;
        }
    }

    private void checkClosed() throws IOException {
        if (this.closed)
            throw new IOException("This instance of the CsvReader class has already been closed.");
    }

    protected void finalize() {
        close(false);
    }

    private class ComplexEscape {
        private static final int UNICODE = 1;

        private static final int OCTAL = 2;

        private static final int DECIMAL = 3;

        private static final int HEX = 4;
    }

    private static char hexToDec(char hex) {
        char result;
        if (hex >= 'a') {
            result = (char)(hex - 97 + 10);
        } else if (hex >= 'A') {
            result = (char)(hex - 65 + 10);
        } else {
            result = (char)(hex - 48);
        }
        return result;
    }

    private class DataBuffer {
        public char[] Buffer = new char[1024];

        public int Position = 0;

        public int Count = 0;

        public int ColumnStart = 0;

        public int LineStart = 0;
    }

    private class ColumnBuffer {
        public char[] Buffer = new char[50];

        public int Position = 0;
    }

    private class RawRecordBuffer {
        public char[] Buffer = new char[500];

        public int Position = 0;
    }

    private class Letters {
        public static final char LF = '\n';

        public static final char CR = '\r';

        public static final char QUOTE = '"';

        public static final char COMMA = ',';

        public static final char SPACE = ' ';

        public static final char TAB = '\t';

        public static final char POUND = '#';

        public static final char BACKSLASH = '\\';

        public static final char NULL = '\000';

        public static final char BACKSPACE = '\b';

        public static final char FORM_FEED = '\f';

        public static final char ESCAPE = '\033';

        public static final char VERTICAL_TAB = '\013';

        public static final char ALERT = '\007';
    }

    private class UserSettings {
        public boolean CaseSensitive = true;

        public char TextQualifier = '"';

        public boolean TrimWhitespace = true;

        public boolean UseTextQualifier = true;

        public char Delimiter = ',';

        public char RecordDelimiter = Character.MIN_VALUE;

        public char Comment = '#';

        public boolean UseComments = false;

        public int EscapeMode = 1;

        public boolean SafetySwitch = false;

        public boolean SkipEmptyRecords = true;

        public boolean CaptureRawRecord = true;
    }

    private class HeadersHolder {
        public String[] Headers = null;

        public int Length = 0;

        public HashMap IndexByName = new HashMap<>();
    }

    private class StaticSettings {
        public static final int MAX_BUFFER_SIZE = 1024;

        public static final int MAX_FILE_BUFFER_SIZE = 4096;

        public static final int INITIAL_COLUMN_COUNT = 10;

        public static final int INITIAL_COLUMN_BUFFER_SIZE = 50;
    }
}
