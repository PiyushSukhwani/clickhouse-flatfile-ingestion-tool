const DataPreview = ({ data, visible, formDataRef, previewDataMessage }) => {
  const selectedColumns = JSON.parse(
    formDataRef.current.get("selectedColumns") || "[]"
  )?.filter((col) => col.selected);

  const previewRows = data.slice(0, 100);

  return (
    <div
      className={`${
        visible ? "block" : "hidden"
      } mb-4 rounded-lg shadow border border-gray-400`}
    >
      <div className="bg-blue-600 text-white px-4 py-3 rounded-t-lg">
        <h5 className="m-0 text-lg font-semibold">Step 5: Data Preview</h5>
      </div>
      <div className="p-4">
        {!data || data.length === 0 ? (
          <div className="text-yellow-700 bg-yellow-100 border border-yellow-300 px-4 py-2 rounded">
            {previewDataMessage}
          </div>
        ) : (
          <>
            <div className="overflow-auto border max-h-[400px] rounded p-3">
              <table className="min-w-full table-fixed text-sm border border-gray-300">
                <thead className="bg-gray-100 sticky -top-4 z-10 shadow ">
                  <tr>
                    {selectedColumns.map((col, idx) => (
                      <th
                        key={idx}
                        className={`px-3 py-2 border-b text-[20px] text-left font-semibold border-gray-300 ${
                          idx !== selectedColumns.length - 1 ? "border-r " : ""
                        }`}
                      >
                        {col.name}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {previewRows.map((row, i) => (
                    <tr key={i} className="odd:bg-white even:bg-gray-100">
                      {selectedColumns.map((col, j, columns) => (
                        <td
                          key={j}
                          className={`px-3 py-2 text-[17px] text-gray-800 border-b border-gray-300 ${
                            j !== selectedColumns.length - 1 ? "border-r " : ""
                          }`}
                        >
                          {row[col.name] ?? ""}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="mt-2 text-gray-600 text-sm">
              Showing {Math.min(100, data.length)} of {data.length} rows
            </div>
          </>
        )}
      </div>
    </div>
  );
};

export default DataPreview;
