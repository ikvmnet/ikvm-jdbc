using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// The base class for a collection of parameters relevant to a <see cref="JdbcCommand"/>.
    /// </summary>
    public abstract class JdbcParameterCollectionBase : DbParameterCollection
    {

        readonly List<JdbcParameter> self = new();

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public JdbcParameterCollectionBase()
        {

        }

        public override int Count => self.Count;

        public override object SyncRoot => this;

        public override int Add(object value)
        {
            if (value is not JdbcParameter p)
                throw new JdbcException("Parameter must be a JdbcParameter.");

            self.Add(p);
            return self.Count - 1;
        }

        public override void AddRange(Array values)
        {
            foreach (var i in values)
                Add(i);
        }

        public override void Clear()
        {
            self.Clear();
        }

        public override bool Contains(object value)
        {
            return value is not JdbcParameter p ? false : self.Contains(p);
        }

        public override bool Contains(string value)
        {
            return self.Any(i => i.ParameterName == value);
        }

        public override void CopyTo(Array array, int index)
        {
            for (int i = 0; i < self.Count; i++)
                array.SetValue(self[i], index + i);
        }

        protected IEnumerator<JdbcParameter> GetEnumeratorImpl()
        {
            return self.GetEnumerator();
        }

        public override IEnumerator GetEnumerator()
        {
            return GetEnumeratorImpl();
        }

        public override int IndexOf(object value)
        {
            return value is not JdbcParameter p ? -1 : self.IndexOf(p);
        }

        public override int IndexOf(string parameterName)
        {
            return self.FindIndex(i => i.ParameterName == parameterName);
        }

        public override void Insert(int index, object value)
        {
            if (value is not JdbcParameter p)
                throw new JdbcException("Parameter must be a JdbcParameter.");

            self.Insert(index, p);
        }

        protected bool RemoveImpl(object value)
        {
            return value is not JdbcParameter p ? false : self.Remove(p);
        }

        public override void Remove(object value)
        {
            if (value is not JdbcParameter p)
                return;

            self.Remove(p);
        }

        public override void RemoveAt(int index)
        {
            self.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            var i = IndexOf(parameterName);
            if (i > -1)
                RemoveAt(i);
        }

        protected override DbParameter GetParameter(int index)
        {
            return self[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            return self.FirstOrDefault(i => i.ParameterName == parameterName);
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            if (value is not JdbcParameter p)
                throw new JdbcException("Parameter must be a JdbcParameter.");

            self[index] = p;
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            RemoveAt(parameterName);
            Add(value);
        }

    }

}
