using System;
using System.Collections.Generic;
using System.Linq;

namespace MapReducePattern
{
    public class MapReduce<KeyInput, ValueInput, KeyOutput, ValueOutput, ValueResult>
    {
        public delegate IEnumerable<KeyValuePair<KeyOutput, ValueOutput>> MapFunction(KeyInput key, ValueInput value);
        public delegate IEnumerable<ValueResult> ReduceFunction(KeyOutput key, IEnumerable<ValueOutput> values);
        private MapFunction _map;
        private ReduceFunction _reduce;
        public MapReduce(MapFunction mapFunction, ReduceFunction reduceFunction)
        {
            _map = mapFunction;
            _reduce = reduceFunction;
        }

        private IEnumerable<KeyValuePair<KeyOutput, ValueOutput>> Map(IEnumerable<KeyValuePair<KeyInput, ValueInput>> input)
        {
            var q = from pair in input
                    from mapped in _map(pair.Key, pair.Value)
                    select mapped;

            return q;
        }

        private IEnumerable<KeyValuePair<KeyOutput, ValueResult>> Reduce(IEnumerable<KeyValuePair<KeyOutput, ValueOutput>> intermediateValues)
        {
            // First, group intermediate values by key 
            var groups = from pair in intermediateValues
                         group pair.Value by pair.Key into g
                         select g;
            // Reduce on each group 
            var reduced = from g in groups
                          let k2 = g.Key
                          from reducedValue in _reduce(k2, g)
                          select new KeyValuePair<KeyOutput, ValueResult>(k2, reducedValue);

            return reduced;
        }

        public IEnumerable<KeyValuePair<KeyOutput, ValueResult>> Execute(IEnumerable<KeyValuePair<KeyInput, ValueInput>> input)
        {
            return Reduce(Map(input));
        }
    }
}
