using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using Orleans.CodeGeneration;
using Orleans.Runtime;

namespace Orleans.Serialization
{
    internal static class BinaryTokenStreamWriterExtensions
    {
        private static readonly Dictionary<RuntimeTypeHandle, SerializationTokenType> typeTokens;
        internal static Dictionary<RuntimeTypeHandle, SerializationTokenType> TypeTokens => typeTokens;

        static BinaryTokenStreamWriterExtensions()
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

        /// <summary>
        /// Write header for an <c>Array</c> to the output stream.
        /// </summary>
        /// <param name="this">The IBinaryTokenStreamReader to read from</param>
        /// <param name="a">Data object for which header should be written.</param>
        /// <param name="expected">The most recent Expected Type currently active for this stream.</param>
        internal static void WriteArrayHeader(this ref BinaryTokenStreamWriter @this, Array a, Type expected = null)
        {
            @this.WriteTypeHeader(a.GetType(), expected);
            for (var i = 0; i < a.Rank; i++)
            {
                @this.Write(a.GetLength(i));
            }
        }

        // Back-references
        internal static void WriteReference(this ref BinaryTokenStreamWriter @this, int offset)
        {
            @this.Write((byte)SerializationTokenType.Reference);
            @this.Write(offset);
        }

        internal static void Write(this ref BinaryTokenStreamWriter @this, SerializationTokenType t)
        {
            @this.Write((byte)t);
        }

        /// <summary> Write a <c>CorrelationId</c> value to the stream. </summary>
        internal static void Write(this ref BinaryTokenStreamWriter @this, CorrelationId id)
        {
            @this.Write(id.ToByteArray());
        }

        /// <summary> Write a <c>ActivationId</c> value to the stream. </summary>
        internal static void Write(this ref BinaryTokenStreamWriter @this, ActivationId id)
        {
            @this.Write(id.Key);
        }

        /// <summary> Write a <c>GrainId</c> value to the stream. </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters")]
        internal static void Write(this ref BinaryTokenStreamWriter @this, GrainId id)
        {
            @this.Write(id.Key);
        }

        /// <summary> Write a <c>ActivationAddress</c> value to the stream. </summary>
        internal static void Write(this ref BinaryTokenStreamWriter @this, ActivationAddress addr)
        {
            @this.Write(addr.Silo ?? SiloAddress.Zero);

            // GrainId must not be null
            @this.Write(addr.Grain);
            @this.Write(addr.Activation ?? ActivationId.Zero);
        }

        internal static void Write(this ref BinaryTokenStreamWriter @this, UniqueKey key)
        {
            @this.Write(key.N0);
            @this.Write(key.N1);
            @this.Write(key.TypeCodeData);
            @this.Write(key.KeyExt);
        }


        /// <summary> Write a type header for the specified Type to the stream. </summary>
        public static void WriteTypeHeader(this ref BinaryTokenStreamWriter @this, Type t, Type expected = null)
        {
            if (t == expected)
            {
                @this.Write((byte)SerializationTokenType.ExpectedType);
                return;
            }

            @this.Write((byte)SerializationTokenType.SpecifiedType);

            if (t.IsArray)
            {
                @this.Write((byte)(SerializationTokenType.Array + (byte)t.GetArrayRank()));
                @this.WriteTypeHeader(t.GetElementType());
                return;
            }

            if (typeTokens.TryGetValue(t.TypeHandle, out var token))
            {
                @this.Write((byte)token);
                return;
            }

            if (t.GetTypeInfo().IsGenericType)
            {
                if (typeTokens.TryGetValue(t.GetGenericTypeDefinition().TypeHandle, out token))
                {
                    @this.Write((byte)token);
                    foreach (var tp in t.GetGenericArguments())
                    {
                        @this.WriteTypeHeader(tp);
                    }
                    return;
                }
            }

            @this.Write((byte)SerializationTokenType.NamedType);
            var typeKey = t.OrleansTypeKey();
            @this.Write(typeKey.Length);
            @this.Write(typeKey);
        }

        public static bool TryWriteSimpleObject(this ref BinaryTokenStreamWriter @this, object obj)
        {
            if (obj == null)
            {
                @this.WriteNull();
                return true;
            }

            if (obj.GetType().TypeHandle.Equals(typeof(bool).TypeHandle))
            {
                @this.Write((bool)obj);
                return true;
            }

            if (typeTokens.TryGetValue(obj.GetType().TypeHandle, out var token))
            {
                switch (token)
                {
                    case SerializationTokenType.Int:
                        @this.Write(token);
                        @this.Write((int)obj);
                        break;
                    case SerializationTokenType.Short:
                        @this.Write(token);
                        @this.Write((short)obj);
                        break;
                    case SerializationTokenType.Long:
                        @this.Write(token);
                        @this.Write((long)obj);
                        break;
                    case SerializationTokenType.Sbyte:
                        @this.Write(token);
                        @this.Write((sbyte)obj);
                        break;
                    case SerializationTokenType.Uint:
                        @this.Write(token);
                        @this.Write((uint)obj);
                        break;
                    case SerializationTokenType.Ushort:
                        @this.Write(token);
                        @this.Write((ushort)obj);
                        break;
                    case SerializationTokenType.Ulong:
                        @this.Write(token);
                        @this.Write((ulong)obj);
                        break;
                    case SerializationTokenType.Byte:
                        @this.Write(token);
                        @this.Write((byte)obj);
                        break;
                    case SerializationTokenType.Float:
                        @this.Write(token);
                        @this.Write((float)obj);
                        break;
                    case SerializationTokenType.Double:
                        @this.Write(token);
                        @this.Write((double)obj);
                        break;
                    case SerializationTokenType.Decimal:
                        @this.Write(token);
                        @this.Write((decimal)obj);
                        break;
                    case SerializationTokenType.String:
                        @this.Write(token);
                        @this.Write((string)obj);
                        break;
                    case SerializationTokenType.Character:
                        @this.Write(token);
                        @this.Write((char)obj);
                        break;
                    case SerializationTokenType.Guid:
                        @this.Write(token);
                        @this.Write((Guid)obj);
                        break;
                    case SerializationTokenType.Date:
                        @this.Write(token);
                        @this.Write((DateTime)obj);
                        break;
                    case SerializationTokenType.TimeSpan:
                        @this.Write(token);
                        @this.Write((TimeSpan)obj);
                        break;
                    case SerializationTokenType.IpAddress:
                        @this.Write(token);
                        @this.Write((IPAddress)obj);
                        break;
                    case SerializationTokenType.IpEndPoint:
                        @this.Write(token);
                        @this.Write((IPEndPoint)obj);
                        break;
                    case SerializationTokenType.GrainId:
                        @this.Write(token);
                        @this.Write((GrainId)obj);
                        break;
                    case SerializationTokenType.ActivationId:
                        @this.Write(token);
                        @this.Write((ActivationId)obj);
                        break;
                    case SerializationTokenType.SiloAddress:
                        @this.Write(token);
                        @this.Write((SiloAddress)obj);
                        break;
                    case SerializationTokenType.ActivationAddress:
                        @this.Write(token);
                        @this.Write((ActivationAddress)obj);
                        break;
                    case SerializationTokenType.CorrelationId:
                        @this.Write(token);
                        @this.Write((CorrelationId)obj);
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
