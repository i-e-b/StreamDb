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

        /// <summary>
        /// Create a bitwise reader over a stream
        /// </summary>
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
                readMask = 0x80;
                if (currentIn < 0) { inRunOut = true; return _runoutBits > 0; }
            }
            else
            {
                readMask >>= 1;
            }
            b=((currentIn & readMask) != 0) ? 1 : 0;
            return true;
        }

        /// <summary>
        /// Returns true when the source bits are exhausted (excludes run-out)
        /// </summary>
        public bool IsEmpty()
        {
            return inRunOut;
        }
    }
}