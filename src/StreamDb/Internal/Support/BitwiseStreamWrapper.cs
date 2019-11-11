using System;
using System.IO;

namespace StreamDb.Internal.Support
{
    /// <summary>
    /// A bitwise wrapper around a byte stream. Also provides run-out
    /// </summary>
    public class BitwiseStreamWrapper {
        private readonly Stream _original;
        private int _runoutBits;

        private bool inRunOut;
        private byte readMask, writeMask;
        private int nextOut, currentIn;

        public BitwiseStreamWrapper(Stream original, int runoutBits)
        {
            _original = original ?? throw new Exception("Must not wrap a null stream");
            _runoutBits = runoutBits;

            inRunOut = false;
            readMask = 1;
            writeMask = 0x80;
            nextOut = 0;
            currentIn = 0;
        }

        /// <summary>
        /// Write the current pending output byte (if any)
        /// </summary>
        public void Flush() {
            if (writeMask == 0x80) return; // no pending byte
            _original.WriteByte((byte)nextOut);
            writeMask = 0x80;
            nextOut = 0;
        }

        /// <summary>
        /// Write a single bit value to the stream
        /// </summary>
        public void WriteBit(bool value){
            if (value) nextOut |= writeMask;
            writeMask >>= 1;

            if (writeMask == 0)
            {
                _original.WriteByte((byte)nextOut);
                writeMask = 0x80;
                nextOut = 0;
            }
        }
        
        /// <summary>
        /// Write a single bit value to the stream
        /// </summary>
        public void WriteBit(int value){
            if (value != 0) nextOut |= writeMask;
            writeMask >>= 1;

            if (writeMask == 0)
            {
                _original.WriteByte((byte)nextOut);
                writeMask = 0x80;
                nextOut = 0;
            }
        }

        /// <summary>
        /// Read a single bit value from the stream.
        /// Returns 1 or 0. Will return all zeros during run-out.
        /// </summary>
        public int ReadBit()
        {
            if (inRunOut)
            {
                if (_runoutBits-- > 0) return 0;
                throw new Exception("End of input stream");
            }

            if (readMask == 1)
            {
                currentIn = _original.ReadByte();
                if (currentIn < 0)
                {
                    inRunOut = true;
                    if (_runoutBits-- > 0) return 0;
                    throw new Exception("End of input stream");
                }
                readMask = 0x80;
            }
            else
            {
                readMask >>= 1;
            }
            return ((currentIn & readMask) != 0) ? 1 : 0;
        }
        
        /// <summary>
        /// Read a single bit value from the stream.
        /// Returns true if data can be read. Does not include run-out
        /// </summary>
        public bool TryReadBit(out int b)
        {
            b=0;
            if (inRunOut) { return false; }
            if (readMask == 1)
            {
                currentIn = _original.ReadByte();
                if (currentIn < 0) { inRunOut = true; return false; }
                readMask = 0x80;
            }
            else
            {
                readMask >>= 1;
            }
            b=((currentIn & readMask) != 0) ? 1 : 0;
            return true;
        }
        
        /// <summary>
        /// Read a single bit value from the stream.
        /// Returns true if data can be read. Includes run-out bits
        /// </summary>
        public bool TryReadBit_RO(out int b)
        {
            b=0;
            if (inRunOut)
            {
                return _runoutBits-- > 0;
            }
            if (readMask == 1)
            {
                currentIn = _original.ReadByte();
                if (currentIn < 0) { inRunOut = true; return _runoutBits > 0; }
                readMask = 0x80;
            }
            else
            {
                readMask >>= 1;
            }
            b=((currentIn & readMask) != 0) ? 1 : 0;
            return true;
        }

        /// <summary>
        /// Read 8 bits from the stream. These might not be aligned to a byte boundary
        /// </summary>
        /// <returns></returns>
        public byte ReadByteUnaligned() {
            byte b = 0;
            for (int i = 0x80; i != 0; i >>= 1)
            {
                if (!TryReadBit(out var v)) break;
                b |= (byte)(i * v);
            }
            return b;
        }

        /// <summary>
        /// Write 8 bits to the stream. These might not be aligned to a byte boundary
        /// </summary>
        public void WriteByteUnaligned(byte value) {
            for (int i = 0x80; i != 0; i >>= 1)
            {
                WriteBit((value & i) != 0);
            }
        }
        
        /// <summary>
        /// Write 8 bits to the stream. These will be aligned to a byte boundary. Extra zero bits may be inserted to force alignment
        /// </summary>
        public void WriteByteAligned(byte value) {
            Flush();
            _original.WriteByte(value);
        }

        /// <summary>
        /// Seek underlying stream to start
        /// </summary>
        public void Rewind()
        {
            _original.Seek(0, SeekOrigin.Begin);
            
            inRunOut = false;
            readMask = 1;
            writeMask = 0x80;
            nextOut = 0;
            currentIn = 0;
        }

        public bool IsEmpty()
        {
            return inRunOut;
        }

        public bool CanRead()
        {
            return _runoutBits > 1;
        }
    }
}