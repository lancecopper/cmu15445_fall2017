SELECT count(*) * 100.0/(SELECT count(*) FROM cases) FROM cases WHERE status LIKE "%Case Closed Statistically%";

