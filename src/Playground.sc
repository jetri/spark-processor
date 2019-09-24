val epochTime5min = 1503368699/300
val long = epochTime5min.toLong
val floor = long * 300


import java.util.{Calendar, TimeZone}

val timeZone = TimeZone.getTimeZone("UTC");
val c = Calendar.getInstance(timeZone)
c.add(Calendar.DAY_OF_MONTH, 1)
c.set(Calendar.HOUR_OF_DAY, 0)
c.set(Calendar.MINUTE, 0)
c.set(Calendar.SECOND, 0)
c.set(Calendar.MILLISECOND, 0)

val currentTime = Calendar.getInstance(timeZone)

val howMany = currentTime .getTimeInMillis - c.getTimeInMillis