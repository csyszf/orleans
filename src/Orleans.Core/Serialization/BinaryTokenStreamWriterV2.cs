using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Orleans.CodeGeneration;
using Orleans.Runtime;

namespace Orleans.Serialization
{
    internal static class BinaryTokenStreamWriterExtensionsV2
    {
        internal static void Write(this BinaryTokenStreamWriterV2 @this, SerializationTokenType t)
        {
            @this.Write((byte)t);
        }
        /// <summary> Write a <c>CorrelationId</c> value to the stream. </summary>
        internal static void Write(this BinaryTokenStreamWriterV2 @this, CorrelationId id)
        {
            @this.Write(id.ToByteArray());
        }

        /// <summary> Write a <c>ActivationAddress</c> value to the stream. </summary>
        internal static void Write(this BinaryTokenStreamWriterV2 @this, ActivationAddress addr)
        {
            @this.Write(addr.Silo ?? SiloAddress.Zero);

            // GrainId must not be null
            @this.Write(addr.Grain);
            @this.Write(addr.Activation ?? ActivationId.Zero);
        }

        internal static void Write(this BinaryTokenStreamWriterV2 @this, UniqueKey key)
        {
            @this.Write(key.N0);
            @this.Write(key.N1);
            @this.Write(key.TypeCodeData);
            @this.Write(key.KeyExt);
        }

        /// <summary> Write a <c>ActivationId</c> value to the stream. </summary>
        internal static void Write(this BinaryTokenStreamWriterV2 @this, ActivationId id)
        {
            @this.Write(id.Key);
        }

        /// <summary> Write a <c>GrainId</c> value to the stream. </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters")]
        internal static void Write(this BinaryTokenStreamWriterV2 @this, GrainId id)
        {
            @this.Write(id.Key);
        }

        /// <summary>
        /// Write header for an <c>Array</c> to the output stream.
        /// </summary>
        /// <param name="this">The IBinaryTokenStreamReader to read from</param>
        /// <param name="a">Data object for which header should be written.</param>
        /// <param name="expected">The most recent Expected Type currently active for this stream.</param>
        internal static void WriteArrayHeader(this BinaryTokenStreamWriterV2 @this, Array a, Type expected = null)
        {
            @this.WriteTypeHeader(a.GetType(), expected);
            for (var i = 0; i < a.Rank; i++)
            {
                @this.Write(a.GetLength(i));
            }
        }

        // Back-references
        internal static void WriteReference(this BinaryTokenStreamWriterV2 @this, int offset)
        {
            @this.Write((byte)SerializationTokenType.Reference);
            @this.Write(offset);
        }
    }
    /// <summary>
    /// Writer for Orleans binary token streams
    /// </summary>
    public ref struct BinaryTokenStreamWriterV2
    {
        private IBufferWriter<byte> _output;
        private Span<byte> _span;
        private int _buffered;

        private static readonly Dictionary<RuntimeTypeHandle, SerializationTokenType> typeTokens;

        static BinaryTokenStreamWriterV2()
        {
            typeTokens = new Dictionary<RuntimeTypeHandle, SerializationTokenType>
            {
                [typeof(bool).TypeHandle] = SerializationTokenType.Boolean,
                [typeof(int).TypeHandle] = SerializationTokenType.Int,
                [typeof(uint).TypeHandle] = SerializationTokenType.Uint,
                [typeof(short).TypeHandle] = SerializationTokenType.Short,
                [typeof(ushort).TypeHandle] = SerializationTokenType.Ushort,
                [typeof(long).TypeHandle] = SerializationTokenType.Long,
                [typeof(ulong).TypeHandle] = SerializationTokenType.Ulong,
                [typeof(byte).TypeHandle] = SerializationTokenType.Byte,
                [typeof(sbyte).TypeHandle] = SerializationTokenType.Sbyte,
                [typeof(float).TypeHandle] = SerializationTokenType.Float,
                [typeof(double).TypeHandle] = SerializationTokenType.Double,
                [typeof(decimal).TypeHandle] = SerializationTokenType.Decimal,
                [typeof(string).TypeHandle] = SerializationTokenType.String,
                [typeof(char).TypeHandle] = SerializationTokenType.Character,
                [typeof(Guid).TypeHandle] = SerializationTokenType.Guid,
                [typeof(DateTime).TypeHandle] = SerializationTokenType.Date,
                [typeof(TimeSpan).TypeHandle] = SerializationTokenType.TimeSpan,
                [typeof(GrainId).TypeHandle] = SerializationTokenType.GrainId,
                [typeof(ActivationId).TypeHandle] = SerializationTokenType.ActivationId,
                [typeof(SiloAddress).TypeHandle] = SerializationTokenType.SiloAddress,
                [typeof(ActivationAddress).TypeHandle] = SerializationTokenType.ActivationAddress,
                [typeof(IPAddress).TypeHandle] = SerializationTokenType.IpAddress,
                [typeof(IPEndPoint).TypeHandle] = SerializationTokenType.IpEndPoint,
                [typeof(CorrelationId).TypeHandle] = SerializationTokenType.CorrelationId,
                [typeof(InvokeMethodRequest).TypeHandle] = SerializationTokenType.Request,
                [typeof(Response).TypeHandle] = SerializationTokenType.Response,
                [typeof(Dictionary<string, object>).TypeHandle] = SerializationTokenType.StringObjDict,
                [typeof(object).TypeHandle] = SerializationTokenType.Object,
                [typeof(List<>).TypeHandle] = SerializationTokenType.List,
                [typeof(SortedList<,>).TypeHandle] = SerializationTokenType.SortedList,
                [typeof(Dictionary<,>).TypeHandle] = SerializationTokenType.Dictionary,
                [typeof(HashSet<>).TypeHandle] = SerializationTokenType.Set,
                [typeof(SortedSet<>).TypeHandle] = SerializationTokenType.SortedSet,
                [typeof(KeyValuePair<,>).TypeHandle] = SerializationTokenType.KeyValuePair,
                [typeof(LinkedList<>).TypeHandle] = SerializationTokenType.LinkedList,
                [typeof(Stack<>).TypeHandle] = SerializationTokenType.Stack,
                [typeof(Queue<>).TypeHandle] = SerializationTokenType.Queue,
                [typeof(Tuple<>).TypeHandle] = SerializationTokenType.Tuple + 1,
                [typeof(Tuple<,>).TypeHandle] = SerializationTokenType.Tuple + 2,
                [typeof(Tuple<,,>).TypeHandle] = SerializationTokenType.Tuple + 3,
                [typeof(Tuple<,,,>).TypeHandle] = SerializationTokenType.Tuple + 4,
                [typeof(Tuple<,,,,>).TypeHandle] = SerializationTokenType.Tuple + 5,
                [typeof(Tuple<,,,,,>).TypeHandle] = SerializationTokenType.Tuple + 6,
                [typeof(Tuple<,,,,,,>).TypeHandle] = SerializationTokenType.Tuple + 7
            };
        }

        /// <summary> Default constructor. </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BinaryTokenStreamWriterV2(IBufferWriter<byte> output, ISerializationContext context)
        {
            this._buffered = 0;
            this._output = output;
            this.Context = context;
            this._span = output.GetSpan();
        }

        public ISerializationContext Context { get; set; }
        public Span<byte> Buffer => this._span;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Flush()
        {
            var buffered = this._buffered;
            if (buffered > 0)
            {
                this._buffered = 0;
                this._output.Advance(buffered);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Advance(int count)
        {
            this._buffered += count;
            this._span = this._span.Slice(count);
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
            this.Flush();
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
            const int width = sizeof(short);
            this.Ensure(width);
            BinaryPrimitives.WriteInt64LittleEndian(this._span, l);
            this.Advance(width);
        }

        /// <summary> Write a <c>sbyte</c> value to the stream. </summary>
        public void Write(sbyte b)
        {
            const int width = 1;
            this.Ensure(width);
            this._span[this._buffered] = (byte)b;
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
            this._span[this._buffered] = b;
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
            // Can be optimized after https://github.com/dotnet/corefxlab/blob/master/src/System.Text.Primitives/System/Text/Encoders/Utf16.cs
            // released.
            var bytes = Encoding.UTF8.GetBytes(s);
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

        // Types

        /// <summary> Write a type header for the specified Type to the stream. </summary>
        /// <param name="t">Type to write header for.</param>
        /// <param name="expected">Currently expected Type for this stream.</param>
        public void WriteTypeHeader(Type t, Type expected = null)
        {
            if (t == expected)
            {
                this.Write((byte)SerializationTokenType.ExpectedType);
                return;
            }

            this.Write((byte)SerializationTokenType.SpecifiedType);

            if (t.IsArray)
            {
                this.Write((byte)(SerializationTokenType.Array + (byte)t.GetArrayRank()));
                this.WriteTypeHeader(t.GetElementType());
                return;
            }

            if (typeTokens.TryGetValue(t.TypeHandle, out var token))
            {
                this.Write((byte)token);
                return;
            }

            if (t.GetTypeInfo().IsGenericType)
            {
                if (typeTokens.TryGetValue(t.GetGenericTypeDefinition().TypeHandle, out token))
                {
                    this.Write((byte)token);
                    foreach (var tp in t.GetGenericArguments())
                    {
                        this.WriteTypeHeader(tp);
                    }
                    return;
                }
            }

            this.Write((byte)SerializationTokenType.NamedType);
            var typeKey = t.OrleansTypeKey();
            this.Write(typeKey.Length);
            this.Write(typeKey);
        }

        // Primitive arrays

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

        /// <summary>
        /// Try to write a simple type (non-array) value to the stream.
        /// </summary>
        /// <param name="obj">Input object to be written to the output stream.</param>
        /// <returns>Returns <c>true</c> if the value was successfully written to the output stream.</returns>
        public bool TryWriteSimpleObject(object obj)
        {
            if (obj == null)
            {
                this.WriteNull();
                return true;
            }

            if (obj.GetType().TypeHandle.Equals(typeof(bool).TypeHandle))
            {
                this.Write((bool)obj);
                return true;
            }

            if (typeTokens.TryGetValue(obj.GetType().TypeHandle, out var token))
            {
                this.Write(token);
                switch (token)
                {
                    case SerializationTokenType.Int:
                        Write((int)obj);
                        break;
                    case SerializationTokenType.Short:
                        Write((short)obj);
                        break;
                    case SerializationTokenType.Long:
                        Write((long)obj);
                        break;
                    case SerializationTokenType.Sbyte:
                        Write((sbyte)obj);
                        break;
                    case SerializationTokenType.Uint:
                        Write((uint)obj);
                        break;
                    case SerializationTokenType.Ushort:
                        Write((ushort)obj);
                        break;
                    case SerializationTokenType.Ulong:
                        Write((ulong)obj);
                        break;
                    case SerializationTokenType.Byte:
                        Write((byte)obj);
                        break;
                    case SerializationTokenType.Float:
                        Write((float)obj);
                        break;
                    case SerializationTokenType.Double:
                        Write((double)obj);
                        break;
                    case SerializationTokenType.Decimal:
                        Write((decimal)obj);
                        break;
                    case SerializationTokenType.String:
                        Write((string)obj);
                        break;
                    case SerializationTokenType.Character:
                        Write((char)obj);
                        break;
                    case SerializationTokenType.Guid:
                        Write((Guid)obj);
                        break;
                    case SerializationTokenType.Date:
                        Write((DateTime)obj);
                        break;
                    case SerializationTokenType.TimeSpan:
                        Write((TimeSpan)obj);
                        break;
                    case SerializationTokenType.IpAddress:
                        Write((IPAddress)obj);
                        break;
                    case SerializationTokenType.IpEndPoint:
                        Write((IPEndPoint)obj);
                        break;
                    case SerializationTokenType.GrainId:
                        this.Write((GrainId)obj);
                        break;
                    case SerializationTokenType.ActivationId:
                        this.Write((ActivationId)obj);
                        break;
                    case SerializationTokenType.SiloAddress:
                        this.Write((SiloAddress)obj);
                        break;
                    case SerializationTokenType.ActivationAddress:
                        this.Write((ActivationAddress)obj);
                        break;
                    case SerializationTokenType.CorrelationId:
                        this.Write((CorrelationId)obj);
                        break;
                    default:
                        return false;
                }
                return true;
            }
            return false;
        }
    }
}
