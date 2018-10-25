using System;
using System.Buffers;
using System.Collections.Generic;
using System.Reflection;

namespace Orleans.Serialization
{
    public static class SerializationContextExtensions
    {
        public static void RecordObject(this ISerializationContext context, object original)
        {
            context.RecordObject(original);
        }

        public static ISerializationContext CreateNestedContext(
            this ISerializationContext context,
            int position,
            IBufferWriter<byte> output)
        {
            return new SerializationContext.NestedSerializationContext(context, position, output);
        }

        public static void SerializeInner<T>(this BinaryTokenStreamWriterV2 @this, T obj)
        {
            SerializationManager.SerializeInner(obj, @this, typeof(T));
        }

        public static void SerializeInner(this BinaryTokenStreamWriterV2 @this, object obj, Type expected)
        {
            SerializationManager.SerializeInner(obj, @this, expected);
        }
    }

    /// <summary>
    /// Maintains context information for current thread during serialization operations.
    /// </summary>
    /// <remarks>
    /// DeepCopier functions in Orleans generated code use the RecordObject method to 
    /// record the mapping of original object to the copied instance of that object
    /// so that object identity can be preserved when serializing .NET object graphs.
    /// </remarks>
    public class SerializationContext : SerializationContextBase, ICopyContext, ISerializationContext
    {
        private struct Record
        {
            public readonly object Copy;
            public readonly int Offset;

            public Record(object copy)
            {
                Copy = copy;
                Offset = 0;
            }

            public Record(int offset)
            {
                Copy = null;
                Offset = offset;
            }
        }

        /// <summary>
        /// Gets the output buffer writer.
        /// </summary>
        public IBufferWriter<byte> BufferWriter { get; set; }

        public uint CurrentReferenceId { get; set; }

        private readonly Dictionary<object, Record> processedObjects;

        public SerializationContext(SerializationManager serializationManager, IBufferWriter<byte> output) :
            base(serializationManager)
        {
            BufferWriter = output;
            processedObjects = new Dictionary<object, Record>(ReferenceEqualsComparer.Instance);
        }

        internal void Reset()
        {
            processedObjects.Clear();
        }

        /// <summary>
        /// Record an object-to-copy mapping into the current serialization context.
        /// Used for maintaining the .NET object graph during serialization operations.
        /// Used in generated code.
        /// </summary>
        /// <param name="original">Original object.</param>
        /// <param name="copy">Copy object that will be the serialized form of the original.</param>
        public void RecordCopy(object original, object copy)
        {
            if (!processedObjects.ContainsKey(original))
            {
                processedObjects[original] = new Record(copy);                
            }
        }

        public void RecordObject(object original, int offset)
        {
            processedObjects[original] = new Record(offset);
        }

        // Returns an object suitable for insertion if this is a back-reference, or null if it's new
        public object CheckObjectWhileCopying(object raw)
        {
            Record record;
            bool found = processedObjects.TryGetValue(raw, out record);
            if (found)
            {
                return record.Copy;
            }

            return null;
        }

        // Returns an offset where the first version of this object was seen, or -1 if it's new
        public int CheckObjectWhileSerializing(object raw)
        {
            Record record;
            bool found = processedObjects.TryGetValue(raw, out record);
            if (found)
            {
                return record.Offset;
            }

            return -1;
        }

        public int CurrentOffset { get; set; }

        public override object AdditionalContext => this.SerializationManager.RuntimeClient;

        public object DeepCopyInner(object original)
        {
            return SerializationManager.DeepCopyInner(original, this);
        }

        public void SerializeInner(object obj, BinaryTokenStreamWriterV2 writer, Type expected)
        {
            SerializationManager.SerializeInner(obj, writer, expected);
        }

        internal class NestedSerializationContext : ISerializationContext
        {
            //private readonly int initialOffset;
            private readonly ISerializationContext parentContext;

            /// <summary>
            /// Creates a new instance of the <see cref="NestedSerializationContext"/> class.
            /// </summary>
            /// <param name="parent">The parent context.</param>
            /// <param name="offset">The absolute offset at which this stream begins.</param>
            /// <param name="output">The output buffer.</param>
            public NestedSerializationContext(ISerializationContext parent, int offset, IBufferWriter<byte> output)
            {
                this.parentContext = parent;
                this.BufferWriter = output;
                this.CurrentOffset = offset;
            }

            public IServiceProvider ServiceProvider => this.parentContext.ServiceProvider;
            public object AdditionalContext => this.parentContext.ServiceProvider;
            public IBufferWriter<byte> BufferWriter { get; }
            public int CurrentOffset { get; set; }
            public void RecordObject(object original, int offset) => this.parentContext.RecordObject(original, offset);
            public int CheckObjectWhileSerializing(object raw) => this.parentContext.CheckObjectWhileSerializing(raw);
        }
    }
}
