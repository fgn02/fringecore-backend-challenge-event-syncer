// challenge.mjs

// Store events: key -> array of event objects
// Each event object: { data: any, timestamp: number, consumedBy: Set<string> }
const eventStore = new Map();

// Store waiting consumers: key -> array of waiting requests { groupId, resolve, timer }
const waiting = new Map();

function cleanUpExpiredEvents(key) {
  const now = Date.now();
  const twoMinutes = 2 * 60 * 1000;
  const events = eventStore.get(key) || [];
  // Filter out old events
  const filtered = events.filter(e => (now - e.timestamp) < twoMinutes);
  eventStore.set(key, filtered);
}
async function blockingGet(key, groupId) {
    console.log("Received blocking-get request:", { key, groupId });
    cleanUpExpiredEvents(key);

    let events = eventStore.get(key) || [];
    const unconsumedEvents = events.filter(e => !e.consumedBy.has(groupId));
    console.log("Unconsumed events:", unconsumedEvents);

    if (unconsumedEvents.length > 0) {
        unconsumedEvents.forEach(e => e.consumedBy.add(groupId));
        console.log("Returning unconsumed events:", unconsumedEvents);
        return unconsumedEvents.map(e => e.data);
    }

    console.log("No events available, waiting...");
    return new Promise((resolve) => {
        const timer = setTimeout(() => {
            removeWaiting(key, groupId, resolve);
            resolve([]);
        }, 30000);

        const entry = { groupId, resolve, timer };
        if (!waiting.has(key)) waiting.set(key, []);
        waiting.get(key).push(entry);
    });
}


function removeWaiting(key, groupId, resolve) {
  // Remove a specific waiting entry from waiting map
  const list = waiting.get(key) || [];
  const newList = list.filter(e => e.resolve !== resolve);
  if (newList.length > 0) {
    waiting.set(key, newList);
  } else {
    waiting.delete(key);
  }
}


async function push(key, data) {
  // Add a new event
  const now = Date.now();
  const event = {
    data,
    timestamp: now,
    consumedBy: new Set(),
  };

  if (!eventStore.has(key)) {
    eventStore.set(key, []);
  }

  // Clean up expired events before pushing new one
  cleanUpExpiredEvents(key);

  eventStore.get(key).push(event);

  // If there are any waiting consumers for this key, try to resolve one of them
  if (waiting.has(key)) {
    const waiters = waiting.get(key);
    if (waiters.length > 0) {
      // Take one waiter (or all) and resolve
      // According to requirements, if multiple consumers in the same group request events,
      // only one will receive. We will resolve the first waiting request we find that can consume events.
      // Actually, let's just resolve all waiting consumers because only one consumer per group will get events,
      // but different groups might also be waiting. We'll handle the consumption logic after they get the events.
      
      // But the spec says: If multiple consumers in the same group request events,
      // only one receives them. If we resolve them all at once, multiple might get the events.
      // Let's just resolve one waiter at a time. The first to get them consumes the events.
      
      const [firstWaiter] = waiters;
      clearTimeout(firstWaiter.timer);
      removeWaiting(key, firstWaiter.groupId, firstWaiter.resolve);

      // Now we have a new event. Let's return all unconsumed events for this group
      cleanUpExpiredEvents(key);
      const events = eventStore.get(key) || [];
      const unconsumed = events.filter(e => !e.consumedBy.has(firstWaiter.groupId));
      unconsumed.forEach(e => e.consumedBy.add(firstWaiter.groupId));
      firstWaiter.resolve(unconsumed.map(e => e.data));
    }
  }
}

export { blockingGet, push };
