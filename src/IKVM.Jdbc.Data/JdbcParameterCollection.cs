using System;
using System.Collections.Generic;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Collection of parameters relevant to a <see cref="JdbcCommand"/>.
    /// </summary>
    public class JdbcParameterCollection : JdbcParameterCollectionBase, IList<JdbcParameter>
    {

        /// <summary>
        /// Gets or sets the parameter at the specified position within this collection.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        JdbcParameter IList<JdbcParameter>.this[int index]
        {
            get => (JdbcParameter)base[index];
            set => base[index] = value;
        }

        /// <summary>
        /// Adds the specified parameter to this collection.
        /// </summary>
        /// <param name="item"></param>
        public void Add(JdbcParameter item)
        {
            base.Add(item);
        }

        /// <summary>
        /// Returns <c>true</c> if this collection contains the specified parameter.
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool Contains(JdbcParameter item)
        {
            if (item is null)
                throw new ArgumentNullException(nameof(item));

            return base.Contains(item);
        }

        /// <summary>
        /// Copies the parameters into the given array starting at the specified index.
        /// </summary>
        /// <param name="array"></param>
        /// <param name="arrayIndex"></param>
        public void CopyTo(JdbcParameter[] array, int arrayIndex)
        {
            base.CopyTo(array, arrayIndex);
        }

        /// <summary>
        /// Returns the index of the specified parameter within this collection.
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public int IndexOf(JdbcParameter parameter)
        {
            return base.IndexOf(parameter);
        }

        /// <summary>
        /// Inserts the parameter into this collection at the specified index.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="parameter"></param>
        public void Insert(int index, JdbcParameter parameter)
        {
            base.Insert(index, parameter);
        }

        /// <summary>
        /// Removes the specified parameter from this collection.
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public bool Remove(JdbcParameter parameter)
        {
            return base.RemoveImpl(parameter);
        }

        /// <summary>
        /// Gets an enumerator that iterates through the parameters in this collection.
        /// </summary>
        /// <returns></returns>
        public new IEnumerator<JdbcParameter> GetEnumerator()
        {
            return GetJdbcEnumerator();
        }

    }

}
