#[cfg(test)]
mod tests {
    use crate::executor::registry::TaskHandlerRegistry;
    use crate::executor::types::{Task, TaskEntry, TaskId, TaskStatus, now_ms};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // ============================================================
    // TEST 1: TaskHandlerRegistry - rejestracja i wykonywanie
    // ============================================================

    #[tokio::test]
    async fn test_registry_register_and_execute() {
        // ARRANGE: Stwórz registry i licznik wywołań
        let registry = TaskHandlerRegistry::new();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        // ACT: Zarejestruj handler
        registry.register("test_handler", move |_task| {
            let count = call_count_clone.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });

        // ASSERT: Handler jest zarejestrowany
        assert!(registry.has_handler("test_handler"));
        assert_eq!(registry.handler_count(), 1);

        // ACT: Wykonaj task
        let task = Task::Execute {
            handler: "test_handler".to_string(),
            payload: serde_json::json!({"test": "data"}),
        };

        let result = registry.execute(&task).await;

        // ASSERT: Handler został wywołany
        assert!(result.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_registry_unknown_handler_returns_error() {
        // ARRANGE
        let registry = TaskHandlerRegistry::new();

        let task = Task::Execute {
            handler: "nieistniejacy_handler".to_string(),
            payload: serde_json::json!({}),
        };

        // ACT
        let result = registry.execute(&task).await;

        // ASSERT: Powinien zwrócić błąd
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unknown task handler")
        );
    }

    #[tokio::test]
    async fn test_registry_handler_can_fail() {
        // ARRANGE
        let registry = TaskHandlerRegistry::new();

        registry.register("failing_handler", |_task| async {
            Err(anyhow::anyhow!("Celowy błąd"))
        });

        let task = Task::Execute {
            handler: "failing_handler".to_string(),
            payload: serde_json::json!({}),
        };

        // ACT
        let result = registry.execute(&task).await;

        // ASSERT
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Celowy błąd"));
    }

    #[tokio::test]
    async fn test_registry_handler_receives_payload() {
        // ARRANGE
        let registry = TaskHandlerRegistry::new();
        let received_payload = Arc::new(tokio::sync::Mutex::new(None));
        let received_clone = received_payload.clone();

        registry.register("payload_handler", move |task| {
            let received = received_clone.clone();
            async move {
                if let Task::Execute { payload, .. } = task {
                    *received.lock().await = Some(payload);
                }
                Ok(())
            }
        });

        let task = Task::Execute {
            handler: "payload_handler".to_string(),
            payload: serde_json::json!({"book_id": "123", "title": "Test Book"}),
        };

        // ACT
        registry.execute(&task).await.unwrap();

        // ASSERT
        let payload = received_payload.lock().await;
        assert!(payload.is_some());
        let p = payload.as_ref().unwrap();
        assert_eq!(p["book_id"], "123");
        assert_eq!(p["title"], "Test Book");
    }

    // ============================================================
    // TEST 2: TaskId
    // ============================================================

    #[test]
    fn test_task_id_is_unique() {
        let id1 = TaskId::new();
        let id2 = TaskId::new();

        assert_ne!(id1.0, id2.0);
    }

    // ============================================================
    // TEST 3: TaskStatus
    // ============================================================

    #[test]
    fn test_task_status_equality() {
        assert_eq!(TaskStatus::Pending, TaskStatus::Pending);
        assert_eq!(TaskStatus::Running, TaskStatus::Running);
        assert_eq!(TaskStatus::Completed, TaskStatus::Completed);

        assert_ne!(TaskStatus::Pending, TaskStatus::Running);

        let failed1 = TaskStatus::Failed {
            error: "test".to_string(),
        };
        let failed2 = TaskStatus::Failed {
            error: "test".to_string(),
        };
        let failed3 = TaskStatus::Failed {
            error: "other".to_string(),
        };

        assert_eq!(failed1, failed2);
        assert_ne!(failed1, failed3);
    }

    // ============================================================
    // TEST 4: TaskEntry serialization
    // ============================================================

    #[test]
    fn test_task_entry_serialization() {
        let entry = TaskEntry {
            task: Task::Execute {
                handler: "index_book".to_string(),
                payload: serde_json::json!({"book_id": "abc123"}),
            },
            status: TaskStatus::Pending,
            assigned_to: None,
            created_at: now_ms(),
            lease_expires: None,
        };

        // Serialize
        let json = serde_json::to_string(&entry).expect("Serialization failed");

        // Deserialize
        let restored: TaskEntry = serde_json::from_str(&json).expect("Deserialization failed");

        assert_eq!(restored.status, TaskStatus::Pending);
        if let Task::Execute { handler, payload } = restored.task {
            assert_eq!(handler, "index_book");
            assert_eq!(payload["book_id"], "abc123");
        } else {
            panic!("Wrong task type");
        }
    }

    // ============================================================
    // TEST 5: Symulacja index_book handlera (bez sieci)
    // ============================================================

    #[tokio::test]
    async fn test_index_book_handler_logic() {
        use crate::search::tokenizer::tokenize_text;
        use std::collections::HashMap;

        // Symulacja index_map jako lokalny HashMap
        let index_map: Arc<tokio::sync::Mutex<HashMap<String, Vec<String>>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        // Symulacja payloadu książki
        let book_payload = serde_json::json!({
            "book_id": "book-001",
            "title": "The Rust Programming Language",
            "author": "Steve Klabnik",
            "language": "en",
            "year": 2019,
            "word_count": 0,
            "unique_words": 0
        });

        // Logika indeksowania (taka sama jak w main.rs)
        let title = book_payload["title"].as_str().unwrap();
        let book_id = book_payload["book_id"].as_str().unwrap();

        let tokens = tokenize_text(title);

        let mut map = index_map.lock().await;
        for token in tokens {
            let book_ids = map.entry(token.clone()).or_insert_with(Vec::new);
            if !book_ids.contains(&book_id.to_string()) {
                book_ids.push(book_id.to_string());
            }
        }
        drop(map);

        // ASSERT: Sprawdź że tokeny są w indeksie
        let map = index_map.lock().await;

        // "rust" powinien być w indeksie
        assert!(
            map.contains_key("rust"),
            "Token 'rust' powinien być w indeksie"
        );
        assert!(map["rust"].contains(&"book-001".to_string()));

        // "programming" też
        assert!(map.contains_key("programming"));
        assert!(map["programming"].contains(&"book-001".to_string()));

        // "language" też
        assert!(map.contains_key("language"));
    }

    // ============================================================
    // TEST 6: Wiele książek z tym samym słowem
    // ============================================================

    #[tokio::test]
    async fn test_multiple_books_same_token() {
        use crate::search::tokenizer::tokenize_text;
        use std::collections::HashMap;

        let index_map: Arc<tokio::sync::Mutex<HashMap<String, Vec<String>>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        // Uwaga: tokenizer filtruje słowa <= 2 znaków
        // więc "Go" nie będzie w indeksie (tylko 2 znaki)
        let books = vec![
            ("book-001", "Rust Programming"),
            ("book-002", "Programming with Golang"), // "golang" zamiast "go"
            ("book-003", "Python Programming"),
        ];

        for (book_id, title) in books {
            let tokens = tokenize_text(title);
            let mut map = index_map.lock().await;

            for token in tokens {
                let book_ids = map.entry(token).or_insert_with(Vec::new);
                if !book_ids.contains(&book_id.to_string()) {
                    book_ids.push(book_id.to_string());
                }
            }
        }

        // ASSERT
        let map = index_map.lock().await;

        // "programming" powinien mieć 3 książki
        assert_eq!(map["programming"].len(), 3);
        assert!(map["programming"].contains(&"book-001".to_string()));
        assert!(map["programming"].contains(&"book-002".to_string()));
        assert!(map["programming"].contains(&"book-003".to_string()));

        // "rust" tylko 1
        assert_eq!(map["rust"].len(), 1);

        // "golang" tylko 1 (zamiast "go" które jest za krótkie)
        assert_eq!(map["golang"].len(), 1);
    }

    // ============================================================
    // TEST 7: Test pełnego flow (registry + index logic)
    // ============================================================

    #[tokio::test]
    async fn test_full_indexing_flow_with_registry() {
        use crate::search::tokenizer::tokenize_text;
        use std::collections::HashMap;

        let registry = TaskHandlerRegistry::new();
        let index_map: Arc<tokio::sync::Mutex<HashMap<String, Vec<String>>>> =
            Arc::new(tokio::sync::Mutex::new(HashMap::new()));

        let index_map_clone = index_map.clone();

        // Rejestracja handlera (podobnie jak w main.rs)
        registry.register("index_book", move |task| {
            let index_map = index_map_clone.clone();
            async move {
                let Task::Execute { payload, .. } = task;

                let book_id = payload["book_id"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing book_id"))?;
                let title = payload["title"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing title"))?;

                let tokens = tokenize_text(title);
                let mut map = index_map.lock().await;

                for token in tokens {
                    let book_ids = map.entry(token).or_insert_with(Vec::new);
                    if !book_ids.contains(&book_id.to_string()) {
                        book_ids.push(book_id.to_string());
                    }
                }

                Ok(())
            }
        });

        // Stwórz i wykonaj task
        let task = Task::Execute {
            handler: "index_book".to_string(),
            payload: serde_json::json!({
                "book_id": "test-book-123",
                "title": "Advanced Rust Patterns"
            }),
        };

        let result = registry.execute(&task).await;
        assert!(result.is_ok(), "Handler powinien się wykonać bez błędów");

        // Sprawdź indeks
        let map = index_map.lock().await;
        assert!(map.contains_key("advanced"));
        assert!(map.contains_key("rust"));
        assert!(map.contains_key("patterns"));
        assert!(map["rust"].contains(&"test-book-123".to_string()));
    }
}
