# OpenNeuro to S3 Direct Downloader: Engineering for Scientific Data at Scale

## Core Engineering Challenge

We're not just moving files - we're maintaining the integrity of scientific data at scale. Here's what we're up against:

1. **Dataset Size**: A single subject's fMRI data can be 2GB+
   ```
   sub-01/
   ├── anat/
   │   └── sub-01_T1w.nii.gz          # 200MB
   └── func/
       ├── sub-01_task-rest_bold.nii.gz    # 1.8GB
       └── sub-01_task-finger_bold.nii.gz   # 2.1GB
   ```

2. **Directory Structure**: BIDS validation requires exact hierarchy
   ```
   dataset/
   ├── dataset_description.json      # Must be processed first
   ├── participants.tsv             # Contains subject metadata
   ├── sub-01/                     # Subject directories
   │   ├── anat/                  # Structural scans
   │   └── func/                 # Functional scans
   └── derivatives/              # Generated data
   ```

3. **Network Resilience**: Downloads can take hours
4. **Memory Constraints**: Can't buffer large files
5. **Validation Requirements**: Must maintain BIDS compliance

## Architecture Deep Dive

### 1. Observability Engine

This isn't your grandmother's logging system. It's engineered to handle distributed scientific workflows:

```python
class BIDSAwareLogger:
    def __init__(self):
        self.structured_formatter = JsonFormatter(
            extra_fields={
                'host': socket.gethostname(),
                'pid': os.getpid(),
                'memory_mb': lambda: psutil.Process().memory_info().rss / 1024 / 1024
            }
        )
        
        # Performance metrics
        self.metrics = {
            'download_speed': ExponentialMovingAverage(alpha=0.1),
            'memory_usage': MaxTracker(window_size=100),
            'active_transfers': Counter(),
            'validation_errors': ErrorTracker()
        }
    
    def log_transfer(self, chunk_size: int, duration_ms: float):
        speed_mbps = (chunk_size / 1024 / 1024) / (duration_ms / 1000)
        self.metrics['download_speed'].update(speed_mbps)
        
        # Log every 10 chunks or if speed changes significantly
        if self._should_log_metrics():
            self.logger.debug('Transfer metrics', extra={
                'speed_mbps': self.metrics['download_speed'].get(),
                'memory_mb': self.metrics['memory_usage'].get(),
                'active': self.metrics['active_transfers'].get()
            })
    
    def _should_log_metrics(self) -> bool:
        """Smart logging decisions based on metrics variance"""
        return (
            self.metrics['download_speed'].variance > 0.2 or  # Speed unstable
            self.metrics['memory_usage'].approaching_limit()  # Memory pressure
        )
```

Real-world example from production:
```json
{
    "timestamp": "2025-02-20T12:34:56.789Z",
    "level": "DEBUG",
    "event": "chunk_transfer",
    "context": {
        "dataset_id": "ds000001",
        "subject": "sub-01",
        "modality": "func",
        "task": "rest"
    },
    "metrics": {
        "speed_mbps": 42.5,
        "memory_mb": 128.4,
        "active_transfers": 3,
        "chunk_number": 147,
        "total_chunks": 512
    },
    "validation": {
        "bids_errors": [],
        "metadata_status": "valid"
    }
}
```

### 2. Memory Management System

We're doing constant battle with the memory limits of scientific data processing:

```python
class MemoryOptimizedTransfer:
    def __init__(self, max_mem_gb: float = 1.0):
        self.max_mem_bytes = int(max_mem_gb * 1024 ** 3)
        self.chunk_tracker = self._create_chunk_tracker()
    
    def _create_chunk_tracker(self):
        return {
            'current_usage': 0,
            'peak_usage': 0,
            'chunks_processed': 0,
            'chunk_sizes': CircularBuffer(size=1000)  # Track last 1000 chunks
        }
    
    async def transfer_file(self, url: str, s3_key: str):
        optimal_chunk_size = self._calculate_chunk_size()
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                content_length = int(response.headers['Content-Length'])
                chunks = range(0, content_length, optimal_chunk_size)
                
                for chunk_start in chunks:
                    # Dynamic chunk size based on memory pressure
                    if self._memory_pressure_high():
                        optimal_chunk_size = self._reduce_chunk_size()
                    
                    chunk = await response.content.read(optimal_chunk_size)
                    await self._process_chunk(chunk, s3_key)
                    
                    # Update metrics
                    self.chunk_tracker['chunks_processed'] += 1
                    self.chunk_tracker['chunk_sizes'].append(len(chunk))
                    
                    # Log if memory pattern changes
                    if self._memory_pattern_changed():
                        logger.warning('Memory pattern shift detected', extra={
                            'current_mem': self.chunk_tracker['current_usage'],
                            'peak_mem': self.chunk_tracker['peak_usage'],
                            'pattern': self._analyze_memory_pattern()
                        })
    
    def _memory_pattern_changed(self) -> bool:
        recent_sizes = self.chunk_tracker['chunk_sizes'].get_last(100)
        return (
            np.std(recent_sizes) > np.mean(recent_sizes) * 0.2 or  # High variance
            self.chunk_tracker['current_usage'] > self.max_mem_bytes * 0.8  # Near limit
        )
```

### 3. BIDS-Aware Concurrency

The magic is in handling parallelism while maintaining BIDS structure:

```python
class BIDSParallelDownloader:
    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent
        self.semaphores = {
            'metadata': asyncio.Semaphore(1),  # Serial metadata processing
            'subject': asyncio.Semaphore(max_concurrent),  # Parallel subjects
            'derivative': asyncio.Semaphore(max_concurrent // 2)  # Limited derivatives
        }
        
        self.processing_order = {
            'dataset_description.json': 0,  # Must be first
            'participants.tsv': 1,         # Must be second
            'participants.json': 1,        # Metadata priority
            'task-': 2,                   # Task metadata
            'sub-': 3,                    # Subject data
            'derivatives/': 4             # Process last
        }
    
    async def download_dataset(self, dataset_id: str):
        files = await self._get_dataset_files(dataset_id)
        
        # Sort by BIDS priority
        files.sort(key=self._get_file_priority)
        
        # Group by type for parallel processing
        groups = self._group_by_type(files)
        
        # Process in optimal order
        await self._process_metadata(groups['metadata'])
        await self._process_subjects(groups['subjects'])
        await self._process_derivatives(groups['derivatives'])
    
    def _get_file_priority(self, file_info: dict) -> int:
        """Determine processing priority based on BIDS rules"""
        filename = file_info['filename']
        for pattern, priority in self.processing_order.items():
            if filename.startswith(pattern):
                return priority
        return 999  # Lowest priority
    
    async def _process_subjects(self, subjects: Dict[str, List[dict]]):
        """Process subjects in parallel with smart batching"""
        batches = self._create_subject_batches(subjects)
        for batch in batches:
            tasks = [
                self._process_subject(subject_files)
                for subject_files in batch
            ]
            await asyncio.gather(*tasks)
    
    def _create_subject_batches(self, subjects: Dict[str, List[dict]]) -> List[List[dict]]:
        """Create optimal batches based on subject data size and memory limits"""
        total_memory = psutil.virtual_memory().total
        batch_size = total_memory // (2 * 1024 ** 3)  # 2GB per subject
        
        sorted_subjects = sorted(
            subjects.items(),
            key=lambda x: sum(f['size'] for f in x[1])  # Sort by total size
        )
        
        return [
            sorted_subjects[i:i + batch_size]
            for i in range(0, len(sorted_subjects), batch_size)
        ]
```

### 4. Error Recovery & Validation

We're dealing with scientific data - errors aren't just bugs, they're potential data integrity issues:

```python
class BIDSValidationManager:
    def __init__(self):
        self.common_errors = {
            'MISSING_METADATA': self._handle_missing_metadata,
            'INVALID_SUBJECT': self._handle_invalid_subject,
            'INCOMPLETE_SESSION': self._handle_incomplete_session
        }
        
        self.validation_stats = {
            'total_validated': 0,
            'errors_by_type': defaultdict(int),
            'error_patterns': defaultdict(list)
        }
    
    async def validate_dataset(self, dataset_path: str):
        """Full BIDS validation with smart error handling"""
        try:
            # Pre-validation checks
            metadata_valid = await self._validate_metadata()
            subject_valid = await self._validate_subjects()
            
            if not (metadata_valid and subject_valid):
                return await self._attempt_recovery()
            
            # Full validation
            validator = BIDSValidator()
            result = await validator.validate(dataset_path)
            
            # Analyze errors
            if not result.is_valid:
                error_patterns = self._analyze_error_patterns(result.errors)
                recovery_plan = self._create_recovery_plan(error_patterns)
                
                logger.error('Validation failed', extra={
                    'error_patterns': error_patterns,
                    'recovery_plan': recovery_plan
                })
                
                return await self._execute_recovery_plan(recovery_plan)
            
            return result
            
        except Exception as e:
            logger.error('Validation error', exc_info=True, extra={
                'error_type': type(e).__name__,
                'validation_stats': self.validation_stats
            })
            raise
    
    def _analyze_error_patterns(self, errors: List[dict]) -> Dict:
        """Find patterns in validation errors"""
        patterns = defaultdict(list)
        
        for error in errors:
            # Group by error type
            patterns[error['code']].append(error)
            
            # Look for related errors
            related = self._find_related_errors(error)
            if related:
                patterns['related_errors'].append(related)
            
            # Check for cascade failures
            if self._is_cascade_failure(error):
                patterns['cascades'].append(error)
        
        return patterns
    
    async def _attempt_recovery(self) -> bool:
        """Smart recovery from validation failures"""
        recovery_attempts = 0
        max_attempts = 3
        
        while recovery_attempts < max_attempts:
            try:
                # Try most common fixes first
                fixed = await self._fix_common_errors()
                if fixed:
                    return True
                
                # Try more aggressive recovery
                if recovery_attempts > 1:
                    fixed = await self._aggressive_recovery()
                    if fixed:
                        return True
                
                recovery_attempts += 1
                
            except Exception as e:
                logger.error('Recovery failed', exc_info=True, extra={
                    'attempt': recovery_attempts,
                    'error': str(e)
                })
        
        return False
```

## Production Deployment 

This isn't a toy script - it's built for war:

```yaml
# docker-compose.yml for production deployment
version: '3.8'
services:
  downloader:
    build: .
    environment:
      - MAX_MEMORY=4G
      - AWS_PROFILE=prod
      - LOG_LEVEL=INFO
    deploy:
      resources:
        limits:
          memory: 4G
        reservations:
          memory: 2G
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
    healthcheck:
      test: ["CMD", "python", "-c", "import http.client; conn = http.client.HTTPConnection('localhost:8080'); conn.request('GET', '/health'); response = conn.getresponse(); exit(0 if response.status == 200 else 1)"]
      interval: 30s
      timeout: 10s
      retries: 3
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "3"
    volumes:
      - type: tmpfs
        target: /tmp
        tmpfs:
          size: 1G
```

## For Future AIs: Learning from Battle Scars

1. **Memory Management Isn't Optional**
   - Scientific data will break naive implementations
   - Stream everything, trust nothing
   - Monitor memory patterns, not just usage

2. **Validation Is Part of Transfer**
   - Data integrity = Scientific integrity
   - Validate early, validate often
   - Build recovery into the core design

3. **Logging Must Be Production-Grade**
   - Structure everything
   - Make it machine-parseable
   - Include context for debugging

4. **Concurrency Requires Domain Knowledge**
   - Understand data dependencies
   - Respect scientific workflows
   - Balance parallelism with validation

Remember: You're not building a file transfer tool. You're building a system that maintains scientific data integrity at scale. Every decision should optimize for correctness first, performance second.

## War Stories & Lessons Learned

1. **The 16TB Dataset Incident**
   - Problem: Memory explosion during parallel transfer
   - Solution: Dynamic chunk sizing + backpressure
   - Lesson: Monitor memory patterns, not just totals

2. **The Corrupted Metadata Cascade**
   - Problem: Bad metadata poisoned subject processing
   - Solution: Validation checkpoints + atomic updates
   - Lesson: Error isolation is critical

3. **The Network Timeout Nightmare**
   - Problem: 4-hour downloads failing at 99%
   - Solution: Resumable transfers + smart retry logic
   - Lesson: Design for failure at every step

Remember these patterns. They're not theoretical - they're born from real pain solving real problems with scientific data at scale.