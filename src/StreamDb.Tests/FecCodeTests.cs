using System;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace StreamDb.Tests
{
    [TestFixture]
    public class FecCodeTests {

        /// <summary>
        /// The first attempt is a simplified version of LDPC that uses simple bit parity rather than galois fields.
        /// This is mostly here to get a feel for things.
        /// </summary>
        [Test]
        public void overlapping_bitwise_parity(){
            // starting with a really simple fixed size block,
            // fixed number of check nodes, and EVEN parity

            // other assumptions:
            // - variable nodes are byte-wise 

            var message = Encoding.ASCII.GetBytes("HELLO 12\0"); // 8 byte message, 1 byte spare space

            // 8 check nodes, each connected to 4 variables. This can be encoded as 8 bits in a byte, stored at the end
            // each input byte has 2 connected parity bytes
            // bit position for message byte (i.e. 0b10000001 picks the chars at 0 and 7)
            // These are all rotations of the same pattern, and each parity bit matches 4 input bytes.
            var parityMatrix = new[]{
                
                0b11000000,
                0b01100000,
                0b00110000,
                0b00011000,
                0b00001100,
                0b00000110,
                0b00000011,
                0b10000001
                /*
                0b11010100,
                0b01101010,
                0b00110101,
                0b10011010,
                0b01001101,
                0b10100110,
                0b01010011,
                0b10101001*/
            };

            Console.WriteLine($"Parity map:\r\n{string.Join("\r\n", parityMatrix.Select(n => BinStr(n,8)))}");

            // Now read the parity into an output
            var parityBits = CalculateParityMatrix(parityMatrix, message);

            // write the parity byte into place
            message[8] = (byte)parityBits;
            Console.WriteLine($"Parity value is {BinStr(parityBits, 8)}");
            Console.WriteLine($"         Message = {HexString(message)} --> '{StringOf(message)}'");

            // Now check our message
            var ok = CheckParityBlock(parityMatrix, message);
            Assert.That(ok, Is.True, "Parity check rejected valid message");

            // Now damage the message a little
            message[4] ^= 8; // flip a single bit
            
            Console.WriteLine($" Damaged message = {HexString(message)} --> '{StringOf(message)}'");

            ok = CheckParityBlock(parityMatrix, message);
//            Assert.That(ok, Is.False, "Failed to detect error");

            // Now damage the message a LOT
            var broken = Copy(message) ?? throw new Exception();
            broken[8] ^= 0xCC;
            broken[1] ^= 0x55;
            broken[0] ^= 0xF0;
            
            Console.WriteLine($"Critical message = {HexString(broken)} --> '{StringOf(broken)}'");

            ok = CheckParityBlock(parityMatrix, broken);
            Assert.That(ok, Is.False, "Failed to detect massive error");

            // Next, try to repair damage?
            // this is the actually tricky bit.
            // for each parity bit, it either agrees or not.
            // an agree is a vote for every set bit. A disagree -- not sure. Maybe no vote, or a small vote against.
            // The parity bits themselves can be damaged, so we want to include that
            // then look at the confidence we have
            // *** We will probably have an issue with everything being 8-bit aligned. A message mix function might help.  ***

            var confidence = new float[8*8]; // a confidence for every bit in the message, excluding the parity bits
            var parityWord = message[8];
            Console.WriteLine();
            for (int pb = 0; pb < 8; pb++) // each parity bit
            {
                var connections = parityMatrix[pb];
                var expected = (parityWord >> pb) & 1;

                // 1. check to see if it matches
                // 2. each message bit covered goes +1.0 for a matched 1, -1.0 for a matched 0
                // 3. each message bit covered goes -0.5 for a disagree 1, +0.5 for a disagree 0

                // calculate the match
                var value = 0;
                for (int j = 0; j < 8; j++) {
                    if (((connections >> j) & 1) == 1) { value += CountBits(message[j]); }
                }

                var match = (value & 1) == expected;

                // with match out of the way, this should be the exact message
                // go back and vote on bits
                for (int j = 0; j < 8; j++)
                { // each code byte
                    for (int q = 0; q < 8; q++)
                    { // each bit in the parity entry
                        if (((connections >> q) & 1) != 1) continue; // only if the check is connected to this byte

                        for (int bit = 0; bit < 8; bit++) // each bit in the byte
                        {
                            var val = ((message[j] >> bit) & 1) - 0.5f;
                            val *= match ? 2 : -1;

                            val = match ? 1 : 0;
                            confidence[(8 * j) /*+ (7 - bit)*/] += val; // should come out as 8*8*2 = 128?
                        }
                    }
                }

            }// end of confidence calculation

            Console.WriteLine("Confidence for damaged data:");
            Console.WriteLine(string.Join("", confidence.Select(ConfidenceChar)));

            Console.WriteLine(string.Join(" ", confidence.Select(f=>f.ToString("0.0"))));
        }

        private static char ConfidenceChar(float f)
        {
            //                   01234567 
            const string ramp = "▁▂▃▄▅▆▇█";

            var x = (int)((f * 4) + 4);
            if (x <= 0) return ramp[0];
            if (x >= 7) return ramp[7];
            return ramp[x];
        }

        private byte[] Copy(byte[] a)
        {
            if (a == null) return null;
            var b = new byte[a.Length];
            a.CopyTo(b,0);
            return b;
        }

        private string StringOf(byte[] message)
        {
            if (message == null) return "<null>";
            return Encoding.ASCII.GetString(message);
        }

        private bool CheckParityBlock(int[] parityMatrix, byte[] message)
        {
            if (parityMatrix == null || message == null) return false;

            int parityBits = message[message.Length - 1]; // always the last byte
            for (int i = 0; i < parityMatrix.Length; i++)
            {
                var connections = parityMatrix[i];
                var value = 0;
                for (int j = 0; j < 8; j++) // pick out characters based on the parity matrix mask
                {
                    var ch = message[j];

                    if (((connections >> j) & 1) == 1)
                    {
                        // include this byte in our check
                        value += CountBits(ch);
                    }
                }

                // calculate parity
                var p = value & 1;
                var expected = (parityBits >> i) & 1;
                if (p != expected) return false;
            }
            return true;
        }

        private static int CalculateParityMatrix(int[] parityMatrix, byte[] message)
        {
            if (parityMatrix == null || message == null) return -1;
            int parityBits = 0;
            for (int i = 0; i < parityMatrix.Length; i++)
            {
                var connections = parityMatrix[i];
                var value = 0;
                for (int j = 0; j < 8; j++) // pick out characters based on the parity matrix mask
                {
                    var ch = message[j];

                    if (((connections >> j) & 1) == 1)
                    {
                        // include this byte in our check
                        value += CountBits(ch);
                    }
                }

                // calculate parity
                var p = value & 1;
                parityBits |= p << i;
            }

            return parityBits;
        }

        private static string HexString(byte[] message)
        {
            if (message == null) return "<null>";
            return string.Join(" ", message.Select(b=>b.ToString("X2")));
        }

        private string BinStr(int v, int l)
        {
            var s = new char[l];
            var e = l - 1;
            for (int i = 0; i < l; i++)
            {
                if (((v >> i) & 1) == 1) s[e-i] = '1';
                else s[e-i] = '0';
            }
            return string.Join("", s);
        }

        public static int CountBits(int value)
        {
            // .Net Core 3 would allow us to use:  `int setBits = System.Runtime.Intrinsics.X86.Popcnt.PopCount(value);`
            var v = (uint)value;
            var count = 0;
            while (v != 0) {
                count++;
                v &= v - 1;
            }
            return count;
        }
    }
}