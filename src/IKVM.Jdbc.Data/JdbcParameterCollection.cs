using System.Collections.Generic;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Collection of parameters relevant to a <see cref="JdbcCommandBase"/>.
    /// </summary>
    public class JdbcParameterCollection : JdbcParameterCollectionBase, IList<JdbcParameter>
    {

        JdbcParameter IList<JdbcParameter>.this[int index]
        {
            get => (JdbcParameter)base[index];
            set => base[index] = value;
        }

        public void Add(JdbcParameter item)
        {
            base.Add(item);
        }

        public bool Contains(JdbcParameter item)
        {
            return base.Contains(item);
        }

        public void CopyTo(JdbcParameter[] array, int arrayIndex)
        {
            base.CopyTo(array, arrayIndex);
        }

        public int IndexOf(JdbcParameter item)
        {
            return base.IndexOf(item);
        }

        public void Insert(int index, JdbcParameter item)
        {
            base.Insert(index, item);
        }

        public bool Remove(JdbcParameter item)
        {
            return base.RemoveImpl(item);
        }

        public new IEnumerator<JdbcParameter> GetEnumerator()
        {
            return base.GetEnumeratorImpl();
        }

    }

}
