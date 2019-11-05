using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;

namespace StreamDb.Internal.Search
{
    public class Map<TIdx, TVal> where TIdx:struct
    {
        [NotNull] private readonly Dictionary<TIdx, TVal> _data;

        [CanBeNull]public TVal this[TIdx index]
        {
            get
            {
                if (_data.ContainsKey(index)) return _data[index];
                return default;
            }
            set
            {
                if (_data.ContainsKey(index)) _data[index] = value;
                else _data.Add(index, value);
            }
        }

        [NotNull]public IEnumerable<TIdx> Keys() => _data.Keys ?? throw new Exception("Map keys entry was invalid");
        public bool Contains(TIdx idx) => _data.ContainsKey(idx);
        public bool IsEmpty() => _data.Count < 1;
        [NotNull]public IEnumerable<KeyValuePair<TIdx, TVal>> All() => _data.Select(a=>a);

        public Map() { _data = new Dictionary<TIdx, TVal>(); }

    }
}