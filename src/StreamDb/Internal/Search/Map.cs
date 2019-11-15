using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;

namespace StreamDb.Internal.Search
{
    public class Map<TIdx, TVal> where TIdx : struct
    {
        [NotNull] private readonly Dictionary<TIdx, TVal> _data;
        private readonly Func<TVal> _generator;

        [CanBeNull]public TVal this[TIdx index]
        {
            get
            {
                if (_data.ContainsKey(index)) return _data[index];
                if (_generator != null) {
                    _data.Add(index, _generator());
                    return _data[index];
                }
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
        public void Clear() => _data.Clear();
        [NotNull]public IEnumerable<KeyValuePair<TIdx, TVal>> All() => _data.Select(a=>a);

        public Map() { _data = new Dictionary<TIdx, TVal>(); }
        public Map(Func<TVal> generator) {
            _data = new Dictionary<TIdx, TVal>();
            _generator=generator;
        }

    }
}