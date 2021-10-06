import asyncio


class Scheduler:
    def __init__(self):
        self.tasks = [asyncio.create_task(self.run, name=f"{self} Worker")]

    async def work(self):
        await self.connected.wait()

        while self.connected.is_set():
            if not self._signals_changed.is_set():
                await self._signals_changed.wait()

            data = await self._queue.get()
            record = Translators.TOPIC_DATA.convert_to_message(data)
            record_type = record.WhichOneof('type')
            record = getattr(record, record_type)

            matching = [topic for topic in self.topic_signals if re.match(fnmatch.translate(topic), record.topic)]

            if not matching:
                log.debug(f"No matching signal found for topic {record.topic}, putting data back in queue")
                await self._queue.put(data)
                self._queue.task_done()
                self._signals_changed.clear()
                continue

            log.debug(f"Matching signal found for topic {record.topic}")
            callbacks = [signal.emit(record) for topic, signal in self.topic_signals.items() if topic in matching]

            await asyncio.gather(*callbacks)
            self._queue.task_done()
