using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Orleans.Runtime;

namespace Orleans.Serialization
{
    /// <summary>
    /// Writer for Orleans binary token streams
    /// </summary>
    public ref struct BinaryTokenStreamWriter
    {
        private IBufferWriter<byte> _output;
        private Span<byte> _span;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BinaryTokenStreamWriter(IBufferWriter<byte> output)
        {
            this.Context = null;
            this._output = output ?? throw new ArgumentNullException(nameof(output));
            this._span = _output.GetSpan();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BinaryTokenStreamWriter(ISerializationContext context)
        {
            this.Context = context;
            //this.Context.CurrentOffset = 0;
            this._output = null;
            this._output = context.BufferWriter ?? throw new ArgumentNullException(nameof(context.BufferWriter));
            this._span = _output.GetSpan();
        }

        public ISerializationContext Context { get; }
        //public Span<byte> Buffer => this._span;

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public void Flush()
        //{
        //    var buffered = this._buffered;
        //    if (buffered > 0)
        //    {
        //        this._buffered = 0;
        //        this._output.Advance(buffered);
        //    }
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Advance(int count)
        {
            if (Context != null)
                this.Context.Advance(count);

            //this._buffered += count;
            this._span = this._span.Slice(count);

            this._output.Advance(count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Ensure(int count = 1)
        {
            if (this._span.Length < count)
            {
                this.EnsureMore(count);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void EnsureMore(int count = 0)
        {
            //this.Flush();
            this._span = this._output.GetSpan(count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Enlarge()
        {
            this.EnsureMore(this._span.Length + 1);
        }

        /// <summary> Write an <c>Int32</c> value to the stream. </summary>
        public void Write(int i)
        {
            const int width = sizeof(int);
            this.Ensure(width);
            BinaryPrimitives.WriteInt32LittleEndian(this._span, i);
            this.Advance(width);
        }

        /// <summary> Write an <c>Int16</c> value to the stream. </summary>
        public void Write(short s)
        {
            const int width = sizeof(short);
            this.Ensure(width);
            BinaryPrimitives.WriteInt16LittleEndian(this._span, s);
            this.Advance(width);
        }

        /// <summary> Write an <c>Int64</c> value to the stream. </summary>
        public void Write(long l)
        {
            const int width = sizeof(long);
            this.Ensure(width);
            BinaryPrimitives.WriteInt64LittleEndian(this._span, l);
            this.Advance(width);
        }

        /// <summary> Write a <c>sbyte</c> value to the stream. </summary>
        public void Write(sbyte b)
        {
            const int width = 1;
            this.Ensure(width);
            //this._span[this.] = (byte)b;
            this._span[0] = (byte)b;
            this.Advance(width);
        }

        /// <summary> Write a <c>UInt32</c> value to the stream. </summary>
        public void Write(uint u)
        {
            const int width = sizeof(uint);
            this.Ensure(width);
            BinaryPrimitives.WriteUInt32LittleEndian(this._span, u);
            this.Advance(width);
        }

        /// <summary> Write a <c>UInt16</c> value to the stream. </summary>
        public void Write(ushort u)
        {
            const int width = sizeof(ushort);
            this.Ensure(width);
            BinaryPrimitives.WriteUInt16LittleEndian(this._span, u);
            this.Advance(width);
        }

        /// <summary> Write a <c>UInt64</c> value to the stream. </summary>
        public void Write(ulong u)
        {
            const int width = sizeof(ulong);
            this.Ensure(width);
            BinaryPrimitives.WriteUInt64LittleEndian(this._span, u);
            this.Advance(width);
        }

        /// <summary> Write a <c>byte</c> value to the stream. </summary>
        public void Write(byte b)
        {
            const int width = 1;
            this.Ensure(width);
            //this._span[this._buffered] = b;
            this._span[0] = b;
            this.Advance(width);
        }

        /// <summary> Write a <c>float</c> value to the stream. </summary>
        public void Write(float f)
        {
            const int width = sizeof(float);
            this.Ensure(width);
            MemoryMarshal.Write(this._span, ref f);
            this.Advance(width);
        }

        /// <summary> Write a <c>double</c> value to the stream. </summary>
        public void Write(double d)
        {
            const int width = sizeof(double);
            this.Ensure(width);
            MemoryMarshal.Write(this._span, ref d);
            this.Advance(width);
        }

        /// <summary> Write a <c>decimal</c> value to the stream. </summary>
        public void Write(decimal d)
        {
            const int width = sizeof(decimal);
            this.Ensure(width);
            MemoryMarshal.Write(this._span, ref d);
            this.Advance(width);
        }

        // Text

        /// <summary> Write a <c>string</c> value to the stream. </summary>
        public void Write(string s)
        {
            if (s == null)
            {
                this.Write(-1);
                return;
            }
            // May be optimized after https://github.com/dotnet/corefxlab/blob/master/src/System.Text.Primitives/System/Text/Encoders/Utf16.cs
            // released.
            var bytes = Encoding.UTF8.GetBytes(s);
            this.Write(bytes.Length);
            this.Write(bytes);
        }

        /// <summary> Write a <c>char</c> value to the stream. </summary>
        public void Write(char c)
        {
            this.Write((ushort)c);
        }

        // Other primitives

        /// <summary> Write a <c>bool</c> value to the stream. </summary>
        public void Write(bool b)
        {
            this.Write((byte)(b ? SerializationTokenType.True : SerializationTokenType.False));
        }

        /// <summary> Write a <c>null</c> value to the stream. </summary>
        public void WriteNull()
        {
            this.Write((byte)SerializationTokenType.Null);
        }


        /// <summary> Write a <c>byte[]</c> value to the stream. </summary>
        public void Write(byte[] b)
        {
            this.Write(b.AsSpan());
        }

        public void Write(ReadOnlySpan<byte> value)
        {
            var input = value;
            while (input.Length > 0)
            {
                this.Enlarge();
                var writeBytes = Math.Min(input.Length, this._span.Length);
                input.Slice(0, writeBytes).CopyTo(this._span);
                this.Advance(writeBytes);
                input = input.Slice(writeBytes);
            }
        }

        public void Write(ReadOnlySequence<byte> value)
        {
            var input = value;
            while (input.Length > 0)
            {
                this.Enlarge();
                var writeBytes = (int)Math.Min(input.Length, this._span.Length);
                input.Slice(0, writeBytes).CopyTo(this._span);
                this.Advance(writeBytes);
                input = input.Slice(writeBytes);
            }
        }

        /// <summary> Write a <c>Int16[]</c> value to the stream. </summary>
        public void Write(short[] i)
        {
            var sour = MemoryMarshal.AsBytes(i.AsSpan());
            this.Write(sour);
        }

        /// <summary> Write a <c>Int32[]</c> value to the stream. </summary>
        public void Write(int[] i)
        {
            var sour = MemoryMarshal.AsBytes(i.AsSpan());
            this.Write(sour);
        }

        /// <summary> Write a <c>Int64[]</c> value to the stream. </summary>
        public void Write(long[] l)
        {
            var sour = MemoryMarshal.AsBytes(l.AsSpan());
            this.Write(sour);
        }

        /// <summary> Write a <c>UInt16[]</c> value to the stream. </summary>
        public void Write(ushort[] i)
        {
            var sour = MemoryMarshal.AsBytes(i.AsSpan());
            this.Write(sour);
        }

        /// <summary> Write a <c>UInt32[]</c> value to the stream. </summary>
        public void Write(uint[] i)
        {
            var sour = MemoryMarshal.AsBytes(i.AsSpan());
            this.Write(sour);
        }

        /// <summary> Write a <c>UInt64[]</c> value to the stream. </summary>
        public void Write(ulong[] l)
        {
            var sour = MemoryMarshal.AsBytes(l.AsSpan());
            this.Write(sour);
        }

        /// <summary> Write a <c>sbyte[]</c> value to the stream. </summary>
        public void Write(sbyte[] l)
        {
            var sour = MemoryMarshal.AsBytes(l.AsSpan());
            this.Write(sour);
        }

        /// <summary> Write a <c>char[]</c> value to the stream. </summary>
        public void Write(char[] l)
        {
            var sour = MemoryMarshal.AsBytes(l.AsSpan());
            this.Write(sour);
        }

        /// <summary> Write a <c>bool[]</c> value to the stream. </summary>
        public void Write(bool[] l)
        {
            var sour = MemoryMarshal.AsBytes(l.AsSpan());
            this.Write(sour);
        }

        /// <summary> Write a <c>double[]</c> value to the stream. </summary>
        public void Write(double[] d)
        {
            var sour = MemoryMarshal.AsBytes(d.AsSpan());
            this.Write(sour);
        }

        /// <summary> Write a <c>float[]</c> value to the stream. </summary>
        public void Write(float[] f)
        {
            var sour = MemoryMarshal.AsBytes(f.AsSpan());
            this.Write(sour);
        }

        // Other simple types

        /// <summary> Write a <c>IPEndPoint</c> value to the stream. </summary>
        public void Write(IPEndPoint ep)
        {
            this.Write(ep.Address);
            this.Write(ep.Port);
        }

        /// <summary> Write a <c>IPAddress</c> value to the stream. </summary>
        public void Write(IPAddress ip)
        {
            if (ip.AddressFamily == AddressFamily.InterNetwork)
            {
                for (var i = 0; i < 12; i++)
                {
                    this.Write((byte)0);
                }
                this.Write(ip.GetAddressBytes()); // IPv4 -- 4 bytes
            }
            else
            {
                this.Write(ip.GetAddressBytes()); // IPv6 -- 16 bytes
            }
        }

        /// <summary> Write a <c>SiloAddress</c> value to the stream. </summary>
        public void Write(SiloAddress addr)
        {
            this.Write(addr.Endpoint);
            this.Write(addr.Generation);
        }

        /// <summary> Write a <c>TimeSpan</c> value to the stream. </summary>
        public void Write(TimeSpan ts)
        {
            this.Write(ts.Ticks);
        }

        /// <summary> Write a <c>DataTime</c> value to the stream. </summary>
        public void Write(DateTime dt)
        {
            this.Write(dt.ToBinary());
        }

        /// <summary> Write a <c>Guid</c> value to the stream. </summary>
        public void Write(Guid id)
        {
            this.Write(id.ToByteArray());
        }

    }
}
