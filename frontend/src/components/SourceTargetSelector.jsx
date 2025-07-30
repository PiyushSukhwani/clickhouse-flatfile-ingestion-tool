import React from "react";

const SourceTargetSelector = ({
  source,
  target,
  onSourceChange,
  onTargetChange,
}) => {
  return (
    <div className="bg-white shadow rounded-lg mb-6 border border-gray-300">
      <div className="bg-blue-500 text-white px-6 py-3 rounded-t-lg">
        <h5 className="text-lg font-medium">
          Step 1: Select Source and Target
        </h5>
      </div>
      <div className="px-6 py-4">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Source Dropdown */}
          <div>
            <label
              htmlFor="sourceType"
              className="block text-md font-mono text-gray-700 mb-1"
            >
              Source:
            </label>
            <select
              id="sourceType"
              className="block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring focus:ring-blue-400"
              onChange={onSourceChange}
            >
              <option value="">-- Select Source --</option>
              <option value="clickhouse">ClickHouse</option>
              <option value="flatfile">Flat File</option>
            </select>
          </div>

          {/* Target Dropdown */}
          <div>
            <label
              htmlFor="targetType"
              className="block text-md font-mono text-gray-700 mb-1"
            >
              Target:
            </label>
            <select
              id="targetType"
              className="block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring focus:ring-blue-500" onChange={onTargetChange}
            >
              <option value="">-- Select Target --</option>
              <option value="clickhouse">ClickHouse</option>
              <option value="flatfile">Flat File</option>
            </select>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SourceTargetSelector;
