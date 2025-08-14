package com.piyush.clickhousefileintegration.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/health")
public class HealthCheckController {

    @GetMapping
    public String healthCheck() {
        return "App is running!";
    }
}

//  @GetMapping("/progress/{jobId}")
// public SseEmitter getProgress(@PathVariable String jobId) {
//     SseEmitter emitter = new SseEmitter(0L); // no timeout
//     progressService.registerEmitter(jobId, emitter);
//     return emitter;
// }

// Inside your ingestion loop (transferDataFromClickHouse), you can send updates:
// if (recordCount % 1000 == 0) {
//     progressService.sendProgress(jobId, recordCount, totalExpectedRows);
// }

// public void sendProgress(String jobId, int current, int total) {
//     SseEmitter emitter = emitters.get(jobId);
//     if (emitter != null) {
//         try {
//             Map<String, Object> payload = Map.of(
//                 "progress", (current * 100) / total,
//                 "recordsProcessed", current
//             );
//             emitter.send(SseEmitter.event().name("progress").data(payload));
//         } catch (IOException e) {
//             emitters.remove(jobId);
//         }
//     }
// }


// const listenToProgress = (jobId, onProgress) => {
//   const eventSource = new EventSource(`${URL}/progress/${jobId}`);
  
//   eventSource.addEventListener("progress", (event) => {
//     const data = JSON.parse(event.data);
//     onProgress(data.progress, data.recordsProcessed);
//   });

//   return eventSource;
// };

// const jobId = Date.now().toString(); // or generate UUID
// const eventSource = listenToProgress(jobId, (progress, count) => {
//   setProgress(progress);
//   setTotalRecords(count);
// });

// // Then trigger ingestion
// executeIngestion(formDataRef, jobId)
//   .then(() => {
//     eventSource.close();
//   })
//   .catch(() => {
//     eventSource.close();
//   });

// const executeIngestion = (formDataRef, jobId) => {
//   const formData = new FormData();

//   const ingestionRequest = {
//     jobId, // NEW
//     sourceType: formDataRef.current.get("sourceType") || "",
//     targetType: formDataRef.current.get("targetType") || "",
//     clickHouseConfig: JSON.parse(formDataRef.current.get("clickHouseConfig") || "{}"),
//     flatFileConfig: JSON.parse(formDataRef.current.get("flatFileConfig") || "{}"),
//     tableName: formDataRef.current.get("tableName") || "",
//     additionalTables: JSON.parse(formDataRef.current.get("additionalTables") || "[]"),
//     joinCondition: formDataRef.current.get("joinCondition") || "",
//     selectedColumns: JSON.parse(formDataRef.current.get("selectedColumns") || "[]"),
//     targetTableName: formDataRef.current.get("targetTableName") || "",
//   };

//   formData.append(
//     "ingestionRequest",
//     new Blob([JSON.stringify(ingestionRequest)], { type: "application/json" })
//   );

//   if (formDataRef.current.get("file")) {
//     formData.append("file", formDataRef.current.get("file"));
//   }

//   return axios
//     .post(`${URL}/execute`, formData, {
//       headers: { "Content-Type": "multipart/form-data" },
//       responseType: ingestionRequest.targetType === "flatfile" ? "blob" : "json",
//     })
//     .then((res) => {
//       if (ingestionRequest.targetType === "flatfile") {
//         const url = window.URL.createObjectURL(res.data);
//         const link = document.createElement("a");
//         link.href = url;
//         link.setAttribute("download", `${ingestionRequest.tableName}.csv`);
//         document.body.appendChild(link);
//         link.click();
//         document.body.removeChild(link);
//         window.URL.revokeObjectURL(url);
//       }
//       return res.data;
//     });
// };


