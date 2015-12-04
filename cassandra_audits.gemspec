$:.push File.expand_path("../lib", __FILE__)

# Maintain your gem's version:
require "cassandra_audits/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = "cassandra_audits"
  s.version     = CassandraAudits::VERSION
  s.authors     = ["blelump"]
  s.email       = ["blelump@ok.ok"]
  s.homepage    = "https://github.com/blelump/cassandra_audits"
  s.summary     = "User actions logging for Rails apps with Cassandra as backend"
  s.description = s.summary

  s.files = Dir["{app,config,db,lib}/**/*"] + ["MIT-LICENSE", "Rakefile", "README.rdoc"]
  s.test_files = Dir["test/**/*"]

  s.add_dependency "rails"

  s.add_development_dependency "sqlite3"
  s.add_development_dependency "rspec-rails"
end
