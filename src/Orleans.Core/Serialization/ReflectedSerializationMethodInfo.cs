namespace Orleans.Serialization
{
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;

    using Orleans.Runtime;

    /// <summary>
    /// Holds references to methods which are used during serialization.
    /// </summary>
    internal class ReflectedSerializationMethodInfo
    {
        /// <summary>
        /// A reference to the <see cref="BinaryTokenStreamWriter.Context"/> getter.
        /// </summary>
        public readonly MethodInfo GetSerializationContextFromStream;

        /// <summary>
        /// A reference to the getter for <see cref="IDeserializationContext.StreamReader"/>.
        /// </summary>
        public readonly MethodInfo GetStreamFromDeserializationContext;

        /// <summary>
        /// A reference to the <see cref="ICopyContext.RecordCopy"/> method.
        /// </summary>
        public readonly MethodInfo RecordObjectWhileCopying;

        /// <summary>
        /// A reference to <see cref="SerializationManager.DeepCopyInner"/>
        /// </summary>
        public readonly MethodInfo DeepCopyInner;

        /// <summary>
        /// A reference to the <see cref="SerializationManager.SerializeInner(object, ref BinaryTokenStreamWriter, Type)"/> method.
        /// </summary>
        public readonly MethodInfo SerializeInner;

        /// <summary>
        /// A reference to the <see cref="SerializationManager.DeserializeInner(Type, IDeserializationContext)"/> method.
        /// </summary>
        public readonly MethodInfo DeserializeInner;

        /// <summary>
        /// A reference to the <see cref="IDeserializationContext.RecordObject(object)"/> method.
        /// </summary>
        public readonly MethodInfo RecordObjectWhileDeserializing;

        /// <summary>
        /// A reference to a method which returns an uninitialized object of the provided type.
        /// </summary>
        public readonly MethodInfo GetUninitializedObject;

        /// <summary>
        /// A reference to <see cref="Type.GetTypeFromHandle"/>.
        /// </summary>
        public readonly MethodInfo GetTypeFromHandle;

        /// <summary>
        /// The <see cref="MethodInfo"/> for the <see cref="Serializer"/> delegate.
        /// </summary>
        public readonly MethodInfo SerializerDelegate;

        /// <summary>
        /// The <see cref="MethodInfo"/> for the <see cref="Deserializer"/> delegate.
        /// </summary>
        public readonly MethodInfo DeserializerDelegate;

        /// <summary>
        /// The <see cref="MethodInfo"/> for the <see cref="DeepCopier"/> delegate.
        /// </summary>
        public readonly MethodInfo DeepCopierDelegate;

        public ReflectedSerializationMethodInfo()
        {
            this.GetUninitializedObject = TypeUtils.Method(() => FormatterServices.GetUninitializedObject(typeof(int)));
            this.GetTypeFromHandle = TypeUtils.Method(() => Type.GetTypeFromHandle(typeof(Type).TypeHandle));
            this.DeepCopyInner = TypeUtils.Method(() => SerializationManager.DeepCopyInner(default(Type), default(ICopyContext)));

            this.SerializeInner = typeof(SerializationManager)
                .GetMethods(BindingFlags.Static | BindingFlags.Public)
                    .Where(m => m.Name == nameof(SerializationManager.SerializeInner)
                   && m.GetParameters().Count() == 3).First();

            this.DeserializeInner = TypeUtils.Method(() => SerializationManager.DeserializeInner(default(Type), default(IDeserializationContext)));

            this.RecordObjectWhileCopying = TypeUtils.Method((ICopyContext ctx) => ctx.RecordCopy(default(object), default(object)));

            this.GetStreamFromDeserializationContext = TypeUtils.Property((IDeserializationContext ctx) => ctx.StreamReader).GetMethod;
            this.GetSerializationContextFromStream = typeof(BinaryTokenStreamWriter).GetProperty("Context").GetMethod;

            this.RecordObjectWhileDeserializing = TypeUtils.Method((IDeserializationContext ctx) => ctx.RecordObject(default(object)));
            this.SerializerDelegate = typeof(Serializer).GetMethod("Invoke");
            this.DeserializerDelegate = TypeUtils.Method((Deserializer del) => del.Invoke(default(Type), default(IDeserializationContext)));
            this.DeepCopierDelegate = TypeUtils.Method((DeepCopier del) => del.Invoke(default(object), default(ICopyContext)));
        }

        internal void SerializerDelegateMethod(Serializer del)
        {
            var writer = new BinaryTokenStreamWriter();
            del.Invoke(default(object), ref writer, default(Type));
        }
    }
}